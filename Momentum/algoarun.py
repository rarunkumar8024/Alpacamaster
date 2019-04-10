import alpaca_trade_api as tradeapi
import requests
import time
from ta import macd
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from pytz import timezone

# Replace these with your API connection info from the dashboard
#base_url = 'Your API URL'
#api_key_id = 'Your API Key'
#api_secret = 'Your API Secret'

api = tradeapi.REST()
session = requests.session()

# We only consider stocks with per-share prices inside this range
min_share_price = 2.0
max_share_price = 13.0
# Minimum previous-day dollar volume for a stock we might consider
min_last_dv = 500000
# Stop limit to default to
default_stop = .95
# How much of our portfolio to allocate to any one position
risk = 0.001
stop_prices = {}
latest_cost_basis = {}
target_prices = {}
temp_stop_prices = {}
channels = ['trade_updates']
divsec = 600


def get_1000m_history_data(symbols):
    print('Getting historical data...')
    minute_history = {}
    c = 0
    for symbol in symbols:
        minute_history[symbol] = api.polygon.historic_agg(
            size="minute", symbol=symbol, limit=1000
        ).df
        c += 1
        print('{}/{}'.format(c, len(symbols)))
        #print("minute history - {}".format(minute_history))
    print('Success.')
    return minute_history


def get_tickers():
    print('Getting current ticker data...')
    tickers = api.polygon.all_tickers()
    print('Success.')
    assets = api.list_assets()
    symbols = [asset.symbol for asset in assets if asset.tradable]
    tickerlist = [ticker for ticker in tickers if (
        ticker.ticker in symbols and
        ticker.lastTrade['p'] >= min_share_price and
        ticker.lastTrade['p'] <= max_share_price and
        ticker.prevDay['v'] * ticker.lastTrade['p'] > min_last_dv and
        ticker.todaysChangePerc >= 3.5
    )]
    ep = api.list_positions()
    epsymbols = [eps.symbol for eps in ep]
    print("epsymbols - {}".format(epsymbols))
    for ticker in tickers:
        if ticker.ticker in epsymbols:
            tickerlist.append(ticker)
    #tickerlist.append(ticker for ticker in tickers if (ticker.ticker in epsymbols))
    #print("TickerList --> {}".format(tickerlist))
    return tickerlist


#def find_stop(current_value, minute_history):
def find_stop(minute_history):
    '''
    print("minute history - {}".format(minute_history))
    print("100 - {}".format(minute_history['low'][-100:]))
    print("dropna - {}".format(minute_history['low'][-100:].dropna()))
    print("resample - {}".format(minute_history['low'][-100:].dropna().resample('5T')))
    print("min - {}".format(minute_history['low'][-100:].dropna().resample('5min').min()))
    '''
    series = minute_history['low'][-100:].resample('5min').min().dropna()
    now = pd.Timestamp.now(tz='US/Eastern')
    series = series[now.floor('1D'):]
    diff = np.nan_to_num(np.diff(series.values))
    low_index = np.where((diff[:-1] <= 0) & (diff[1:] > 0))[0] + 1
    if len(low_index) > 0:
        return series[low_index[-1]] - 0.01
    #return current_value * default_stop


def run(tickers, market_open_dt, market_close_dt):
    

    # Update initial state with information from tickers
    pos_counter = 0
    volume_today = {}
    prev_closes = {}
    for ticker in tickers:
        symbol = ticker.ticker
        prev_closes[symbol] = ticker.prevDay['c']
        volume_today[symbol] = ticker.day['v']

    symbols = [ticker.ticker for ticker in tickers]
    print('Tracking {} symbols.'.format(len(symbols)))
    print('Symbols - {}'.format(symbols))
    minute_history = get_1000m_history_data(symbols)

    portfolio_value = float(api.get_account().portfolio_value)

    open_orders = {}
    positions = {}

    # Cancel any existing open orders on watched symbols
    existing_orders = api.list_orders(limit=500)
    for order in existing_orders:
        if order.symbol in symbols:
            api.cancel_order(order.id)
    
    global temp_stop_prices
    global stop_prices
    stop_prices = {}
    latest_cost_basis = {}

    # Track any positions bought during previous executions
    existing_positions = api.list_positions()
    for position in existing_positions:
        if position.symbol in symbols:
            positions[position.symbol] = float(position.qty)
            # Recalculate cost basis and stop price
            latest_cost_basis[position.symbol] = float(position.cost_basis)
            #stop_prices[position.symbol] = (
            #    float(position.cost_basis) * default_stop
            stop_prices[position.symbol] = 0
            stop_prices[position.symbol] = find_stop(minute_history[position.symbol])
            if stop_prices[position.symbol] == 0:
                stop_prices[position.symbol] = float(position.cost_basis) * default_stop
            if temp_stop_prices.get(position.symbol,0) and (float(temp_stop_prices[position.symbol]) > float(stop_prices[position.symbol])):
                stop_prices[position.symbol] = temp_stop_prices[position.symbol]
                print("Trailing stop loss value retrieved - {}, calc stop loss - {}".format(
                    temp_stop_prices[position.symbol],stop_prices[position.symbol]))
            target_prices[position.symbol] = portfolio_value
            print("Existing position - {}, with stop_prices -{}".format(position.symbol,stop_prices[position.symbol]))
    temp_stop_prices = {}        

    # Keep track of what we're buying/selling
    
    partial_fills = {}
    #find_stop_loss = {}

    # Establish streaming connection
    #conn = tradeapi.stream2.StreamConn() #key_id=api_key_id, secret_key=api_secret)
    conn = tradeapi.StreamConn() #key_id=api_key_id, secret_key=api_secret)
    
    # Use trade updates to keep track of our portfolio
    @conn.on(r'trade_updates')
    async def handle_trade_update(conn, channel, data):
        symbol = data.order['symbol']
        last_order = open_orders.get(symbol)
        if last_order is not None:
            event = data.event
            if event == 'partial_fill':
                print("Inside trade_update routine - partial fill")
                qty = int(data.order['filled_qty'])
                if data.order['side'] == 'sell':
                    qty = qty * -1
                if data.order['side'] == 'buy':
                    stop_price = find_stop(minute_history[symbol])
                    stop_prices[symbol] = stop_price
                    target_prices[symbol] = data.close + (
                    (data.close - stop_price) * 3
                    )
                positions[symbol] = (
                    positions.get(symbol, 0) - partial_fills.get(symbol, 0)
                )
                partial_fills[symbol] = qty
                positions[symbol] += qty
                open_orders[symbol] = data.order
            elif event == 'filled':
                print("Inside trade_update routine - filled")
                qty = int (data.order['filled_qty'])
                if data.order['side'] == 'sell':
                    qty = qty * -1
                if data.order['side'] == 'buy':
                    stop_price = find_stop(minute_history[symbol])
                    stop_prices[symbol] = stop_price
                    target_prices[symbol] = data.close + (
                    (data.close - stop_price) * 3
                    )  
                    print("Trade updates Buy symbol - {}, close price - {}, stop_price - {}, target_price - {}".format(
                    symbol, data.close, stop_prices[symbol], target_prices[symbol]))    
                positions[symbol] = (
                    positions.get(symbol, 0) - partial_fills.get(symbol, 0)
                )
                partial_fills[symbol] = 0
                positions[symbol] += qty
                if positions[symbol] == 0:
                    removeconn(symbol)
                open_orders[symbol] = None
            elif event == 'canceled' or event == 'rejected':
                print("Inside trade_update routine - cancelled or rejected")
                qty = int(data.order['filled_qty'])
                if data.order['side'] == 'buy':
                    qty = qty * -1
                positions[symbol] = (
                    positions.get(symbol, 0) - partial_fills.get(symbol, 0)
                )
                partial_fills[symbol] = 0
                positions[symbol] += qty
                if positions[symbol] == 0:
                    del positions[symbol]
                    removeconn(symbol)
                open_orders[symbol] = None

    @conn.on(r'A\..*')
    async def handle_second_bar(conn, channel, data):
        #print("inside handle_second_bar")
        symbol = data.symbol
        #print("market_open_dt - {}, market_close_dt - {} ".format(market_open_dt, market_close_dt))
        #print("data.start - {}".format(data.start))
        #print("data - {}".format(data))
        # First, aggregate 1s bars for up-to-date MACD calculations
        #ts = data.start
        global channels
        global divsec
        ts = pd.Timestamp.now(tz='US/Eastern')
        ts -= timedelta(seconds=ts.second, microseconds=ts.microsecond)
        since_market_open = ts - market_open_dt
        until_market_close = market_close_dt - ts
        if ts.time() <= pd.Timestamp('09:30',tz='US/Eastern').time():
            channels = ['trade_updates']
            print("A - 9:30")
            run_ws(conn,channels)
            print("Connections closed from A")
        elif ts.time() >= pd.Timestamp('16:00',tz='US/Eastern').time():
            channels = ['trade_updates']
            print("A - 16")
            run_ws(conn,channels)
            print("Connections closed from A")
        if since_market_open.seconds // divsec == 1:
            channels = ['trade_updates']
            print("A - divsec")
            run_ws(conn,channels)
            print("Connections closed from A and getting tickers, divsec - {}".format(divsec))      
            divsec += 600
            run(get_tickers(), market_open_dt, market_close_dt)
        
        try:
            current = minute_history[data.symbol].loc[ts]
        except KeyError:
            current = None
        new_data = []
        if current is None:
            new_data = [
                data.open,
                data.high,
                data.low,
                data.close,
                data.volume
            ]
        else:
            new_data = [
                current.open,
                data.high if data.high > current.high else current.high,
                data.low if data.low < current.low else current.low,
                data.close,
                current.volume + data.volume
            ]
        minute_history[symbol].loc[ts] = new_data

        # Next, check for existing orders for the stock
        existing_order = open_orders.get(symbol)
        if existing_order is not None:
            # Make sure the order's not too old
            submission_ts = existing_order.submitted_at.astimezone(
                #timezone('America/New_York')
                timezone('US/Eastern')
            )
            order_lifetime = ts - submission_ts
            if order_lifetime.seconds // 60 > 1:
                # Cancel it so we can try again for a fill
                api.cancel_order(existing_order.id)
            return

        position = positions.get(symbol, 0)
        try:
            #position = int(api.get_position(symbol).qty)
            if position > 0:
                # Update stop price and target price
                stoplossprice = float (default_stop * data.close)
                stopprice = stop_prices.get(symbol,0)
                if stoplossprice > stopprice:
                    stop_prices[symbol] = stoplossprice
                
                print("symbol - {}, close price - {}, stop_price - {}, stoploss - {}, target_price - {}".format(
                symbol, data.close, stop_prices[symbol], stoplossprice, target_prices[symbol]))    
                try:
                    if int(api.get_position(symbol).qty) <= 0:
                        print("Position {} for symbol - {}".fomat(api.get_position(symbol).qty,symbol))
                        removeconn(symbol)
                except Exception as e:
                        print("except1")
                        if e.__eq__("position does not exist"):
                            del positions[symbol]
                            removeconn(symbol)
                        if e.__ne__("position does not exist"):
                            print(e)
                        return
            #print("Since Market Open - {}, until_market_close.seconds - {}".format(since_market_open.seconds, until_market_close.seconds))
        except Exception as e:
            print("except2")
            print("symbol - {}, stop_price - {}, stoploss - {}".format(
                symbol, stop_prices[symbol], stoplossprice))  
            if e.__eq__("position does not exist"):
                del positions[symbol]
                print(e)   
            return     
        '''
        if  until_market_close.seconds // 60 < 1:
            print("Closing connections")
            channels = []
            run_ws(conn,channels)
        '''
        # Now we check to see if it might be time to buy or sell
        
        if (
            since_market_open.seconds // 60 > 10 
            and
            #since_market_open.seconds // 60 < 60
            until_market_close.seconds // 60 > 1
        ):
            # Check for buy signals

            # See if we've already bought in first
            position = positions.get(symbol, 0)
            try:
                #position = int(api.get_position(symbol).qty)
                if position > 0:
                    # Sell for a loss if it's fallen below our stop price
                    # Sell for a loss if it's below our cost basis and MACD < 0
                    # Sell for a profit if it's above our target price
                    hist = macd(
                        minute_history[symbol]['close'].dropna(),
                        n_fast=13,
                        n_slow=21
                    )
                    if (
                        data.close <= stop_prices[symbol] or
                        (data.close >= target_prices[symbol] and hist[-1] <= 0) or
                        (data.close <= latest_cost_basis[symbol] and hist[-1] <= 0)
                    ):
                        print('Submitting sell for {} shares of {} at {}, stop_price - {}, hist[-1] - {}, target_prices - {}, costbasis - {}'.format(
                            position, symbol, data.close, stop_prices[symbol], hist[-1], target_prices[symbol], latest_cost_basis[symbol]
                        ))
                        print("stop_price ")
                        try:
                            o = api.submit_order(
                            symbol=symbol, qty=str(position), side='sell',
                            type='limit', time_in_force='day',
                            limit_price=str(data.close)
                            )
                            open_orders[symbol] = o
                            latest_cost_basis[symbol] = data.close
                            #del positions[symbol]
                        except Exception as e:
                            print("except3")
                            if e.__ne__("position does not exist"):
                                print(e)
                            if e.__eq__("position does not exist"):
                                del positions[symbol]
                            return
                    return
            except Exception as e:
                print("except4")
                if e.__ne__("position does not exist"):
                    print(e)
                if e.__eq__("position does not exist"):
                    del positions[symbol]
                return

            # See how high the price went during the first 15 minutes
            lbound = market_open_dt
            ubminutes = int (since_market_open.seconds // 60)
            #ubound = lbound + timedelta(minutes=15)
            ubound = lbound + timedelta(minutes=ubminutes)
            high_day = 0
            high_60m
            try:
                high_day = minute_history[symbol][lbound:ubound]['high'].max()
                high_60m = minute_history[symbol][-60:]['high'].max()
                print("symbol - {}, day high - {}, High (60mins) - {}".format(symbol,high_day,high_60m))
            except Exception as e:
                #print("except5")
                # Because we're aggregating on the fly, sometimes the datetime
                # index can get messy until it's healed by the minute bars
                return

            # Get the change since yesterday's market close
            daily_pct_change = (
                (data.close - prev_closes[symbol]) / prev_closes[symbol]
            )
            if (
                daily_pct_change > .04 and
                data.close > high_60m and
                high_60m > high_day and
                volume_today[symbol] > 30000
            ):
                #if float (api.get_account.buying_power) < data.close:
                #    return
                    
                # check for a positive, increasing MACD
                hist = macd(
                    minute_history[symbol]['close'].dropna(),
                    n_fast=12,
                    n_slow=26
                )
                if (
                    hist[-1] < 0 or
                    not (hist[-3] < hist[-2] < hist[-1])
                ):
                    return
                hist = macd(
                    minute_history[symbol]['close'].dropna(),
                    n_fast=40,
                    n_slow=60
                )
                if hist[-1] < 0 or np.diff(hist)[-1] < 0:
                    return
                
                #Skip the buying if the stock price is greater than the Account Buying power
                if float(api.get_account().buying_power) < data.close:
                    return

                # Stock has passed all checks; figure out how much to buy
                #print("minute_history[symbol] - {}".format(minute_history[symbol]))
                stop_price = float(data.close * default_stop) #= find_stop(minute_history[symbol])
                stop_prices[symbol] = stop_price
                target_prices[symbol] = data.close + (
                    (data.close - stop_price) * 3
                )                
                shares_to_buy = portfolio_value * risk // (
                    data.close - stop_price
                )
                if shares_to_buy == 0:
                    shares_to_buy = 1
                shares_to_buy -= positions.get(symbol, 0)
                if shares_to_buy <= 0 or (float(api.get_account().cash) <= data.close):
                    #print("skipping the buy for {}, at the price -{} with portfolio avl cash - {}".format(
                    #    symbol,data.close,float(api.get_account().cash)))
                    return
                print('Submitting buy for {} shares of {} at {}, with set stop price - {} and Target price - {}'.format(
                    shares_to_buy, symbol, data.close, stop_prices[symbol], target_prices[symbol]
                ))
                try:
                    o = api.submit_order(
                        symbol=symbol, qty=str(shares_to_buy), side='buy',
                        type='limit', time_in_force='day',
                        limit_price=str(data.close)
                    )
                    open_orders[symbol] = o
                    latest_cost_basis[symbol] = data.close
                    positions[symbol] = int(shares_to_buy)
                except Exception as e:
                    print("except6")
                    print(e)
                    return
                    '''if e.__eq__("insufficient buying power"):
                        if open_orders.get(symbol,0) >= 0:
                            del open_orders[symbol]
                        if latest_cost_basis.get(symbol,0) >= 0:
                            del latest_cost_basis[symbol]
                    '''
                return
            
    # Replace aggregated 1s bars with incoming 1m bars
    @conn.on(r'AM\..*')
    async def handle_minute_bar(conn, channel, data):
        #print("market_open_dt - {}, market_close_dt - {} ".format(market_open_dt, market_close_dt))
        #print("data.start - {}".format(data.start))
        #print("data - {}".format(data))
        global channels
        ts = pd.Timestamp.now(tz='US/Eastern')
        if ts.time() <= pd.Timestamp('09:30', tz='US/Eastern').time():
            channels = ['trade_updates']
            print("AM - 9:30")
            run_ws(conn,channels)
            print("Connections closed from AM")
        elif ts.time() >= pd.Timestamp('16:00', tz='US/Eastern').time():
            channels = ['trade_updates']
            print("AM - 16")
            run_ws(conn,channels)
            print("Connections closed from AM")
        ts = data.start
        ts -= timedelta(microseconds=ts.microsecond)
        minute_history[data.symbol].loc[ts] = [
            data.open,
            data.high,
            data.low,
            data.close,
            data.volume
        ]
        volume_today[data.symbol] += data.volume
        #existing_positions = api.list_positions()
        '''
        for symbol in positions:
            qty = api.get_position(symbol).qty
            try:
                if qty > 0:
                    positions[symbol] = qty
                if qty <= 0:
                    del positions[symbol]
            except:
                del positions[symbol]
        '''      
    
    global channels
    for symbol in symbols:
        symbol_channels = ['A.{}'.format(symbol), 'AM.{}'.format(symbol)]
        channels += symbol_channels
    
    print('Watching {} symbols.'.format(len(symbols)))
    print("Channels - {}".format(channels))
    #conn.register(channels,run)
    print("channel ini")
    run_ws(conn, channels)


def removeconn(symbol):
    try:
        
        if stop_prices.get(symbol,0) >= 0:
            del stop_prices[symbol]
        if latest_cost_basis.get(symbol,0) >= 0:
            del latest_cost_basis[symbol]
        if target_prices.get(symbol,0) >= 0:
            del target_prices[symbol]
        #if symbol in symbols:
        #    symbols.remove(symbol)
        #if len(symbols) <= 0:
        #    conn.close()
        #conn.deregister([
        #    'A.{}'.format(symbol),
        #    'AM.{}'.format(symbol)
        #])
    except Exception as e:
        print("except7")
        if e.__ne__("position does not exist"):
            print(e)
        #print(e)
        return

# Handle failed websocket connections by reconnecting
def run_ws(conn, channels):
    try:
        conn.run(channels)
    except Exception as e:
        print("except8")
        if e.__ne__("position does not exist"):
            print(e)
        #print(e)
        conn.close
        run_ws(conn, channels)


def main():
    done = None
    global temp_stop_prices
    global stop_prices
    # Get when the market opens or opened today
    while True:
       
#       clock = api.get_clock()
#        now = clock.timestamp
#        print ("done - {}, clock - {}".format(done, clock))
#        if (clock.is_open and done != now.strftime('%Y-%m-%d')):
#        #    print ("Inside clock.is open")

        #nyc = timezone('America/New_York')
        nyc = timezone('US/Eastern')
        today = datetime.today().astimezone(nyc)
        today_str = datetime.today().astimezone(nyc).strftime('%Y-%m-%d')
        calendar = api.get_calendar(start=today_str, end=today_str)[0]
        market_open = today.replace(
                hour=calendar.open.hour,
                minute=calendar.open.minute,
                second=0
        )    
        market_open = market_open.astimezone(nyc)
        market_close = today.replace(
                hour=calendar.close.hour,
                minute=calendar.close.minute,
                second=0
        )
        market_close = market_close.astimezone(nyc)
            
            # Wait until just before we might want to trade
        current_dt = datetime.today().astimezone(nyc)
        since_market_open = current_dt - market_open
            
        #print("Current_dt - {}, Since Market Open - {}".format(current_dt,since_market_open))

        clock = api.get_clock()
        now = clock.timestamp
        #print("clock - {}".format(clock))
        if clock.is_open and done != today_str:
            print("clock - {}".format(clock))
            temp_stop_prices = stop_prices
            while since_market_open.seconds // 60 <= 10:
                get_tickers()
                # Cancel any existing open orders on watched symbols
                existing_orders = api.list_orders(limit=500)
                for order in existing_orders:
                    if order.side == 'buy':
                        print("Cancelling pre-market {} order - {}".format(order.side, order.symbol))
                        api.cancel_order(order.id)
                time.sleep(60)
                current_dt = datetime.today().astimezone(nyc)
                since_market_open = current_dt - market_open
            #current_dt = datetime.today().astimezone(nyc)
            #since_market_open = current_dt - market_open
            done = today_str
            divsec = 600
            run(get_tickers(), market_open, market_close)
        
        #else:
        #    run(get_tickers(), market_open, market_close)
        '''
            ts = pd.Timestamp.now(tz='US/Eastern')
            symbols = {'CTSH'}
            min_his = get_1000m_history_data(symbols)
            find_stop(min_his['CTSH'])
        '''
        time.sleep(60)
        
        #    done = now.strftime('%Y-%m-%d')
        #time.sleep(60)
        #print("done - {}".format(done))  
        if clock.is_open == False and done == now.strftime('%Y-%m-%d'):
            channels = ['trade_updates']
            run_ws(conn,channels)
            print("Connections closed")
            done = None
        #    time.sleep(54000)
        

def trailingstoploss(symbol,marketprice):
    try:
        #marketprice = getcurrentprice(symbol)
        stoplossprice = float (default_stop * marketprice)
        print("trailingstop loss - symbol - {}".format(symbol))
        print("stop price - {}".format(stop_prices[symbol]))
        if stoplossprice > stop_prices[symbol]:
            stop_prices[symbol] = stoplossprice
            print("stoploss value updated stoploss - {}, current price - {}".format(stop_prices[symbol],marketprice))
    except Exception as e:
        print("except9")
        logger.error(e)
