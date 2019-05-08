import alpaca_trade_api as tradeapi
import iexfinance as iex
import pandas as pd
import logging
import time
from ta import macd
import numpy as np
from .TVSignal import *

api = tradeapi.REST()
NY = 'US/Eastern'
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARNING)

def get_tickers(min_share_price, max_share_price, min_last_dv):
    fday_hist = {}
    fday_sym = []
    print('Getting current ticker data...')
    tickers = api.polygon.all_tickers()
    #print("all tickers - {}".format(tickers))
    print('Success.')
    assets = api.list_assets()
    #print("assets - {}".format(assets))
    symbols = [asset.symbol for asset in assets if asset.tradable]
    if (len(assets) < 1 or len(symbols) < 1):
        print("No Assests or Symbols to process")
        return
    print("getting tickerlist")
    tickerlist = [ticker for ticker in tickers if (
        ticker.ticker in symbols and
        ticker.prevDay['c'] >= float (min_share_price) and
        ticker.prevDay['c'] <= float (max_share_price) and
        ticker.prevDay['v'] * ticker.prevDay['c'] > float(min_last_dv) #and
        #len(ticker.ticker) <= 4
        #ticker.todaysChangePerc >= 3.5
        )]
    #print("Tickerlist - {}".format(tickerlist))
    Universe = [ticker.ticker for ticker in tickerlist ]
    #print("Universe from Pipe - {}, {}".format(len(Universe),Universe))
    day_hist = prices(Universe)
    #print(day_hist)
    #print("back from prices routine")
    for symbol in Universe:
        try:
            #print("Processing ############# - {}".format(symbol))
            hist = macd(day_hist[symbol]['close'].dropna(), n_fast=12, n_slow=26)
            if (hist[-1] < 0 or (not (hist[-3] < hist[-2] < hist[-1]))):
                continue
            hist = macd(day_hist[symbol]['close'].dropna(), n_fast=40, n_slow=60)
            if hist[-1] < 0 or np.diff(hist)[-1] < 0:
                continue
            #print("complete - {}".format(symbol))
            exchange = api.get_asset(symbol).exchange
            tvsignal = get_TVsignal(symbol,exchange)
            #print ("TV --> {}".format(tvsignal))
            # Select only the stocks which satisfy TV Overall signal in Buy or Strong Buy and the RSI is within 30 to 70
            if (float(tvsignal[1]) < 0.5) or \
            (float(tvsignal[1]) >= 0.5 and (float(tvsignal[3]) < 30 or float(tvsignal[3]) > 70)):
                continue
            
            fday_hist[symbol] = day_hist[symbol]
            fday_sym.append(symbol)
            #fday_hist[symbol]['tvsig'] = tvsignal[1]
            #print("symbol - {}, Exchange - {}, TVSignal - {}".format(symbol, exchange, fday_hist[symbol]['tvsig'][-1]))
        except Exception as e:
            print ("{} - {}".format(symbol,e))
            
    #print(fday_hist)
    print("From pipe -> {} - {}".format(len(fday_sym), fday_sym))
    
    
    return fday_hist
                



def _get_prices(symbols, end_dt, max_workers=5):
    '''Get the map of DataFrame price data from Alpaca's data API.'''
    try:
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
            #print("idx value - {}".format(idx))

        return barset.df
    except Exception as e:
            #print("inside pipe exception")
            logger.error(e)
            return barset.df

def prices(symbols):
    '''Get the map of prices in DataFrame with the symbol name key.'''
    now = pd.Timestamp.now(tz=NY)
    end_dt = now
    #if now.time() >= pd.Timestamp('00:00', tz=NY).time():
    end_dt = now - \
            pd.Timedelta(now.strftime('%H:%M:%S')) - pd.Timedelta('1 minute')
    #print("calling get prices")
    return _get_prices(symbols, end_dt)

#get_tickers('1','10','50000')
