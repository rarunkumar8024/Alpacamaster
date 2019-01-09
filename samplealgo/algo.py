import alpaca_trade_api as tradeapi
import pandas as pd
import time
import logging
import concurrent.futures
import requests
from datetime import datetime
#import backtrader as bt
from .btest import simulate
from .universe import Universe
from .pipe import make_pipeline
#import pipeline_live as pipeline


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
#logging.getLogger(__name__).setLevel(logging.warning)

NY = 'America/New_York'
api = tradeapi.REST()
stopprice = {}
lossfactor = 0.95
orders = []
MaxCandidates = 25

def _dry_run_submit(*args, **kwargs):
    logging.info(f'submit({args}, {kwargs})')
# api.submit_order =_dry_run_submit


def _get_polygon_prices(symbols, end_dt, max_workers=5):
    '''Get the map of DataFrame price data from polygon, in parallel.'''

    #start_dt = end_dt - pd.Timedelta('1200 days')
    start_dt = end_dt - pd.Timedelta('50 days')
    _from = start_dt.strftime('%Y-%m-%d')
    to = end_dt.strftime('%Y-%m-%d')

    def historic_agg(symbol):
        return api.polygon.historic_agg(
            'day', symbol, _from=_from, to=to).df.sort_index()

    with concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers) as executor:
        results = {}
        future_to_symbol = {
            executor.submit(
                historic_agg,
                symbol): symbol for symbol in symbols}
        for future in concurrent.futures.as_completed(future_to_symbol):
            symbol = future_to_symbol[future]
            try:
                results[symbol] = future.result()
            except Exception as exc:
                logger.warning(
                    '{} generated an exception: {}'.format(
                        symbol, exc))
        #print("Inside get_polygon_prices - results - {}".format(results))
        return results

def getcurrentprice(symbol):
    ''' Get the map of current prices in DataFrame with the symbol key'''
    cprice = api.polygon.last_trade(symbol)
    print("Current price for stock {} -{}".format(symbol,cprice))    
    return (cprice.price)

def prices(symbols):
    '''Get the map of prices in DataFrame with the symbol name key.'''
    now = pd.Timestamp.now(tz=NY)
    end_dt = now
    if now.time() >= pd.Timestamp('09:30', tz=NY).time():
        end_dt = now - \
            pd.Timedelta(now.strftime('%H:%M:%S')) - pd.Timedelta('1 minute')
    #print("Inside prices -- Now -{}, end_dt - {}, symbols - {}".format(now, end_dt, symbols))
    return _get_polygon_prices(symbols, end_dt)


def calc_scores(price_map, dayindex=-1):
    '''Calculate scores based on the indicator and
    return the sorted result.
    '''
    diffs = {}
    param = 10
    #print("Inside Calc_scores - Listing Price_map details")
    #print("{}".format(price_map))
    for symbol, df in price_map.items():
        if len(df.close.values) <= param:
            continue
        ema = df.close.ewm(span=param).mean()[dayindex]
        last = df.close.values[dayindex]
        diff = (last - ema) / last
        diffs[symbol] = diff

    return sorted(diffs.items(), key=lambda x: x[1])


def get_orders(api, price_map, todays_order, position_size=100, max_positions=25):
    '''Calculate the scores with the universe to build the optimal
    portfolio as of today, and extract orders to transition from
    current portfolio to the calculated state.
    '''
    # rank the stocks based on the indicators.
    ranked = calc_scores(price_map)
    to_buy = set()
    to_sell = set()
    account = api.get_account()
    print("Inside get_orders")
    print("Ranked stocks - {}".format(ranked))
    print("account cash - {}".format(account.cash))
    # take the top one twentieth out of ranking,
    # excluding stocks too expensive to buy a share
    #for symbol, _ in ranked[:len(ranked) // 20]:
    for symbol, _ in ranked[:len(ranked)]:
        price = float(price_map[symbol].close.values[-1])
        if price > float(account.cash):
            continue
        to_buy.add(symbol)
    print("To Buy - {}".format(to_buy))
    # now get the current positions and see what to buy,
    # what to sell to transition to today's desired portfolio.
    positions = api.list_positions()
    logger.info(positions)
    holdings = {p.symbol: p for p in positions}
    holding_symbol = set(holdings.keys())
    todays_order_array = {q['symbol'] for q in todays_order}
    print("Todays order array - {}".format(todays_order_array))
    to_sell = holding_symbol - to_buy - todays_order_array
    to_buy = to_buy - holding_symbol - todays_order_array

    orders = []
    print("Holding positions - {}".format(positions))
    print("To Sell - {}".format(to_sell))

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
    print("Max to buy - {}".format(max_to_buy))
    cash = float (account.cash)
    for symbol in to_buy:
        if max_to_buy <= 0:
            break
        currentprice = getcurrentprice(symbol)
        max_shares = (cash /float (max (price_map[symbol].close.values[-1],currentprice)))
        
        print ("max_shares - {}".format(max_shares))
        #shares = (min (position_size, max_shares) // float(price_map[symbol].close.values[-1]))
        shares = int(min (position_size, max_shares))
        print ("calculated Shares - {}".format(shares))
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
    This is an important step since the orders aren't filled atomically,
    which means if your buys come first with littme cash left in the account,
    the buy orders will be bounced.  In order to make the transition smooth,
    we sell first and wait for all the sell orders to fill and then submit
    buy orders.
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
                type='market',
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

#class SmaCross(bt.SignalStrategy):
#    def __init__(self):
#        sma1, sma2 = bt.ind.SMA(period=10), bt.ind.SMA(period=30)
#        crossover = bt.ind.CrossOver(sma1, sma2)
#        self.signal_add(bt.SIGNAL_LONG, crossover)



def main():
    '''The entry point. Goes into an infinite loop and
    start trading every morning at the market open.'''
    done = None
    #sold_today = {}
    todays_order = []
    test_flag = False
    logging.info('start running')
    #set initial stop loss values for the stocks in the portfolio, just in case algo was had a problem and need to restart
    positions = api.list_positions()
    logger.info(positions)
    holdings = {p.symbol: p for p in positions}
    holding_symbol = set(holdings.keys())
    for symbol in holding_symbol:
        set_stoploss(symbol)
    # end of initial stop loss assignment
    #Initial calls
    pipeout = make_pipeline(MaxCandidates)
    stocks_best = pipeout[pipeout['stocks_best']].index.tolist()
    #price_map = prices(Universe)
    print("Best stocks - {}".format(stocks_best))
    price_map = prices(stocks_best)
         
    while True:
        
        now = pd.Timestamp.now(tz=NY)
        if (now.time() >= pd.Timestamp('09:00', tz=NY).time() and done != now.strftime('%Y-%m-%d')) or test_flag:
            pipeout = make_pipeline(MaxCandidates)
            stocks_best = pipeout[pipeout['stocks_best']].index.tolist()
            #price_map = prices(Universe)
            print("Best stocks - {}".format(stocks_best))
            
        # clock API returns the server time including
        # the boolean flag for market open
        clock = api.get_clock()
        now = clock.timestamp

        if (clock.is_open and done != now.strftime('%Y-%m-%d')) or test_flag:
            todays_order = []
            price_map = prices(stocks_best)
            orders = get_orders(api, price_map,todays_order)
            trade(orders)
            todays_order = orders
            print("todays_order - {}, orders - {}".format(todays_order, orders))
            # flag it as done so it doesn't work again for the day
            # TODO: this isn't tolerant to the process restart
            done = now.strftime('%Y-%m-%d')
            logger.info(f'done for {done}')
        time.sleep(60)    
        if clock.is_open or test_flag:
            stoploss()
            if float(api.get_account().cash) >= 1.0:
                orders = get_orders(api, price_map,todays_order)
                trade(orders)
                todays_order = orders

def set_stoploss(symbol):
    try:
        position = api.get_position(symbol)
        costbasis = float(position.avg_entry_price)
        if symbol in stopprice:
            print("stopprice already set for the stock {}, recalculating".format(symbol))
            stopprice[symbol] = lossfactor * costbasis
        else:
            stopprice[symbol] = lossfactor * costbasis

        #stopprice[symbol] = max(stopprice[symbol] if symbol in stopprice else 0, costbasis)
        print("stoploss for {} is {}".format(symbol, stopprice[symbol]))
    except Exception as e:
        stopprice[symbol] = 0
        print("Problem with set stoploss for {} is {}".format(symbol, stopprice[symbol]))
        logger.error(e)
    
def stoploss():
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
            stoplossprice = float (lossfactor * marketprice)
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
                trade(orders)
                
    except Exception as e:
        logger.error(e)
            
        
