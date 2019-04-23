import alpaca_trade_api as tradeapi
import pandas as pd
import time
import logging
from .pipe import get_tickers
from .universe import UniverseT

#from .universe import Universe

# We only consider stocks with per-share prices inside this range
min_share_price = 2.0
max_share_price = 13.0
# Minimum previous-day dollar volume for a stock we might consider
min_last_dv = 500000
# Stop limit to default to
default_stop = .95
# How much of our portfolio to allocate to any one position
risk = 0.1
done = None
todays_order = {}

api = tradeapi.REST()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARNING)

NY = 'US/Eastern'

def _dry_run_submit(*args, **kwargs):
    logging.info(f'submit({args}, {kwargs})')
# api.submit_order =_dry_run_submit


def _get_prices(symbols, end_dt, max_workers=5):
    '''Get the map of DataFrame price data from Alpaca's data API.'''

    start_dt = end_dt - pd.Timedelta('50 days')
    start = start_dt.strftime('%Y-%m-%d')
    end = end_dt.strftime('%Y-%m-%d')

    def get_barset(symbols):
        return api.get_barset(
            symbols,
            'day',
            limit = 50,
            start=start,
            end=end
        )

    # The maximum number of symbols we can request at once is 200.
    barset = None
    idx = 0
    while idx <= len(symbols) - 1:
        if barset is None:
            barset = get_barset(symbols[idx:idx+200])
        else:
            barset.update(get_barset(symbols[idx:idx+200]))
        idx += 200

    return barset.df


def prices(symbols):
    '''Get the map of prices in DataFrame with the symbol name key.'''
    now = pd.Timestamp.now(tz=NY)
    end_dt = now
    if now.time() >= pd.Timestamp('09:30', tz=NY).time():
        end_dt = now - \
            pd.Timedelta(now.strftime('%H:%M:%S')) - pd.Timedelta('1 minute')
    return _get_prices(symbols, end_dt)


def calc_scores(price_df, dayindex=-1):
    '''Calculate scores based on the indicator and
    return the sorted result.
    '''
    diffs = {}
    param = 10
    for symbol in price_df.columns.levels[0]:
        df = price_df[symbol]
        if len(df.close.values) <= param:
            continue
        ema = df.close.ewm(span=param).mean()[dayindex]
        last = df.close.values[dayindex]
        diff = (last - ema) / last
        diffs[symbol] = diff

    return sorted(diffs.items(), key=lambda x: x[1])


def get_orders(api, price_df, position_size=100, max_positions=5):
    global todays_order
    '''Calculate the scores within the universe to build the optimal
    portfolio as of today, and extract orders to transition from our
    current portfolio to the desired state.
    '''
    # rank the stocks based on the indicators.
    ranked = calc_scores(price_df)
    to_buy = set()
    to_sell = set()
    account = api.get_account()

    for symbol, _ in ranked[:len(ranked)]:
        price = float(price_df[symbol].close.values[-1])
        if price > float(account.cash):
            continue
        to_buy.add(symbol)

    # now get the current positions and see what to buy,
    # what to sell to transition to today's desired portfolio.
    positions = api.list_positions()
    logger.info(positions)
    holdings = {p.symbol: p for p in positions}
    holding_symbol = set(holdings.keys())
    if todays_order is None:
        todays_order = set()
    print("unprocessed To Buy - {}".format(to_buy))
    print("Current positions - {}, Today's order - {}".format(holding_symbol, todays_order))
    to_sell = holding_symbol - to_buy - todays_order
    to_buy = to_buy - holding_symbol - todays_order
    print("processed To sell - {}, To Buy - {}".format(to_sell, to_buy))
    orders = []

    # if a stock is in the portfolio, and not in the desired
    # portfolio, sell it
    for symbol in to_sell:
        shares = holdings[symbol].qty
        orders.append({
            'symbol': symbol,
            'qty': shares,
            'side': 'sell',
        })
        logger.info(f'order(sell): {symbol} for {shares}')

    # likewise, if the portfoio is missing stocks from the
    # desired portfolio, buy them. We sent a limit for the total
    # position size so that we don't end up holding too many positions.
    max_to_buy = max_positions - (len(positions) - len(to_sell))
    cash = float (account.cash)
    for symbol in to_buy:
        if max_to_buy <= 0:
            break
        currentprice = getcurrentprice(symbol)
        if currentprice > cash:
            continue
        max_shares = ((cash * risk) /float (max (price_df[symbol].close.values[-1],currentprice)))
        shares = int(min (position_size, max_shares))
        #shares = position_size // float(price_df[symbol].close.values[-1])
        if shares == 0.0:
            continue
        
        orders.append({
            'symbol': symbol,
            'qty': shares,
            'side': 'buy',
        })
        logger.info(f'order(buy): {symbol} for {shares}')
        max_to_buy -= 1
        cash -= (shares * currentprice)
    return orders


def trade(orders, wait=30):
    '''This is where we actually submit the orders and wait for them to fill.
    Waiting is an important step since the orders aren't filled automatically,
    which means if your buys happen to come before your sells have filled,
    the buy orders will be bounced. In order to make the transition smooth,
    we sell first and wait for all the sell orders to fill before submitting
    our buy orders.
    '''

    # process the sell orders first
    sells = [o for o in orders if o['side'] == 'sell']
    for order in sells:
        try:
            logger.info(f'submit(sell): {order}')
            api.submit_order(
                symbol=order['symbol'],
                qty=order['qty'],
                side='sell',
                type='market',
                time_in_force='day',
            )
            symbol = order['symbol']
            if symbol in stopprice: 
                print("Removed {} from stop price with stoploss as {}".format(symbol,stopprice[symbol]))
                del stopprice[symbol]
        except Exception as e:
            logger.error(e)
    count = wait
    while count > 0:
        pending = api.list_orders()
        if len(pending) == 0:
            logger.info(f'all sell orders done')
            break
        logger.info(f'{len(pending)} sell orders pending...')
        time.sleep(1)
        count -= 1

    # process the buy orders next
    buys = [o for o in orders if o['side'] == 'buy']
    for order in buys:
        try:
            logger.info(f'submit(buy): {order}')
            api.submit_order(
                symbol=order['symbol'],
                qty=order['qty'],
                side='buy',
                type='limit',
                time_in_force='day',
            )
            # Add stoploss entry for the ordered symbol
            set_stoploss(order['symbol']) 
        except Exception as e:
            logger.error(e)
    count = wait
    while count > 0:
        pending = api.list_orders()
        if len(pending) == 0:
            logger.info(f'all buy orders done')
            break
        logger.info(f'{len(pending)} buy orders pending...')
        time.sleep(1)
        count -= 1


def main():
    global done
    global todays_order
    flag_sym = True
    flag_inirun = True
    flag_test = True
    logging.info('start running')
    while True:
        try:
            # clock API returns the server time including
            # the boolean flag for market open
            clock = api.get_clock()
            now = clock.timestamp
            #print("done - {}, now - {}, pd timestamp - {}, flag_sym - {}".format(done,now.time(), pd.Timestamp('08:09',tz=NY).time(), flag_sym))
            if (done != now.strftime('%Y-%m-%d') 
                and now.time() > pd.Timestamp('09:00',tz=NY).time() 
                and now.time() < pd.Timestamp('09:30',tz=NY).time() 
                and flag_sym) or flag_inirun: 
                
                Universe = get_tickers(min_share_price,max_share_price,min_last_dv)
                flag_sym = False
                flag_inirun = False
            if flag_test:
                Universe = UniverseT
                print("Universe Test - {}".format(Universe))
            #print("after get tickers - {}".format(Universe))
            if (clock.is_open and done != now.strftime('%Y-%m-%d')) or flag_test:
                todays_order = set()
                if len(Universe) == 0:
                    Universe = get_tickers(min_share_price,max_share_price,min_last_dv)
                price_df = prices(Universe)
                orders = get_orders(api, price_df)
                trade(orders)
                todays_order = gettodaysorder()
                print("todays_order - {}, orders - {}".format(todays_order, orders))
            
                # flag it as done so it doesn't work again for the day
                # TODO: this isn't tolerant to process restarts, so this
                # flag should probably be saved on disk
                done = now.strftime('%Y-%m-%d')
                flag_sym = True
                logger.info(f'done for {done}')
            if clock.is_open or flag_test:
                stoploss()
                if float(api.get_account().cash) >= 1.0:
                    orders = get_orders(api, price_df)
                    trade(orders)
                    todays_order = gettodaysorder()
            
            time.sleep(60)
        except Exception as e:
            print(e)
            main()

def getcurrentprice(symbol):
    ''' Get the map of current prices in DataFrame with the symbol key'''
    cprice = api.polygon.last_trade(symbol)
    print("Current price for stock {} -{}".format(symbol,cprice))    
    return (cprice.price)

def set_stoploss(symbol):
    try:
        position = api.get_position(symbol)
        costbasis = float(position.avg_entry_price)
        if symbol in stopprice:
            print("stopprice already set for the stock {}, recalculating".format(symbol))
            stopprice[symbol] = default_stop * costbasis
        else:
            stopprice[symbol] = default_stop * costbasis

        #stopprice[symbol] = max(stopprice[symbol] if symbol in stopprice else 0, costbasis)
        print("stoploss for {} is {}".format(symbol, stopprice[symbol]))
    except Exception as e:
        stopprice[symbol] = 0
        print("Problem with set stoploss for {} is {}".format(symbol, stopprice[symbol]))
        logger.error(e)
    
def stoploss():
    #global done
    try:
        positions = api.list_positions()
        logger.info(positions)
        holdings = {p.symbol: p for p in positions}
        holding_symbol = set(holdings.keys())
                
        for symbol in holding_symbol:
            if symbol in stopprice: 
                None 
            else: 
                set_stoploss(symbol)
            marketprice = getcurrentprice(symbol)
            stoplossprice = float (default_stop * marketprice)
            costbasis = float(holdings[symbol].avg_entry_price)
            print("Calc stoplossprice - {}, stoploss - {}, costbasis - {}, current price - {}".format(stoplossprice,stopprice[symbol],costbasis,marketprice))
            
            if stoplossprice > stopprice[symbol]:
                stopprice[symbol] = stoplossprice
                print("stoploss value updated stoploss - {}, costbasis - {}, current price - {}".format(stopprice[symbol],costbasis,marketprice))
            if marketprice <= stopprice[symbol]:
                # Market price is less than stop loss price.  Sell the stock.
                shares = holdings[symbol].qty
                orders.append({
                    'symbol': symbol,
                    'qty': shares,
                    'side': 'sell',
                })
                logger.info(f'Stoploss order(sell): {symbol} for {shares}')
                orders = get_orders(api, price_df)
                trade(orders)
                #done = None
                
    except Exception as e:
        logger.error(e)

def gettodaysorder():
    global todays_order
    try:
        now = pd.Timestamp.now(tz='US/Eastern')
        now1= pd.Timestamp.now(tz='UTC')
        until_dt = now1.strftime('%Y-%m-%dT%H:%M:%SZ')
        after_dt = now - \
            pd.Timedelta(now.strftime('%H:%M:%S')) - pd.Timedelta('1 minute')
        after_dt = after_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        print("after_dt -{}, until_dt - {}".format(after_dt, until_dt))
        #orders4mtoday = api.list_orders(status='all', after=after_dt) #,until=until_dt)
        orders4mtoday = api.list_orders(status='all', after=after_dt) #,until=until_dt)
        #print("orders4mtoday - {}".format(orders4mtoday))
        order_symbols = set()
        for o in orders4mtoday:
            order_symbols.add(o.symbol)

        return order_symbols
    except Exception as e:
        logger.error(e)  