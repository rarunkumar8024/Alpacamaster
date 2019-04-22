import alpaca_trade_api as tradeapi
import iexfinance as iex

api = tradeapi.REST()

def get_tickers(min_share_price, max_share_price, min_last_dv):
    print('Getting current ticker data...')
    tickers = api.polygon.all_tickers()
    #print("all tickers - {}".format(tickers))
    print('Success.')
    assets = api.list_assets()
    #print("assets - {}".format(assets))
    symbols = [asset.symbol for asset in assets if asset.tradable]
    tickerlist = [ticker for ticker in tickers if (
        ticker.ticker in symbols and
        ticker.lastTrade['p'] >= min_share_price and
        ticker.lastTrade['p'] <= max_share_price and
        ticker.prevDay['v'] * ticker.lastTrade['p'] > min_last_dv #and
        #ticker.todaysChangePerc >= 3.5
        )]
    #print("Tickerlist - {}".format(tickerlist))
    Universe = [ticker.ticker for ticker in tickers]
    print("Universe from Pipe - {}".format(Universe))
    return Universe
