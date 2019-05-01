import requests, json, time, datetime

def mlog(market, *text):
	text = [str(i) for i in text]
	text = " ".join(text)
	return text
	#datestamp = str(datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3])
	#print("[{} {}] - {}".format(datestamp, market, text))


def get_signal(market, pipe, candle, exchange, url):
	headers = {'User-Agent': 'Mozilla/5.0'}

	payload =	{
					"symbols": {
						"tickers": ["{}:{}".format(exchange,market)],
						"query": { "types": [] }
					},
					"columns": [
						"Recommend.Other{}{}".format(pipe,candle),
						"Recommend.All{}{}".format(pipe,candle),
						"Recommend.MA{}{}".format(pipe,candle),
						"RSI{}{}".format(pipe,candle),
						"RSI[1]{}{}".format(pipe,candle),
						"Stoch.K{}{}".format(pipe,candle),
						"Stoch.D{}{}".format(pipe,candle),
						"Stoch.K[1]{}{}".format(pipe,candle),
						"Stoch.D[1]{}{}".format(pipe,candle),
						"CCI20{}{}".format(pipe,candle),
						"CCI20[1]{}{}".format(pipe,candle),
						"ADX{}{}".format(pipe,candle),
						"ADX+DI{}{}".format(pipe,candle),
						"ADX-DI{}{}".format(pipe,candle),
						"ADX+DI[1]{}{}".format(pipe,candle),
						"ADX-DI[1]{}{}".format(pipe,candle),
						"AO{}{}".format(pipe,candle),
						"AO[1]{}{}".format(pipe,candle),
						"Mom{}{}".format(pipe,candle),
						"Mom[1]{}{}".format(pipe,candle),
						"MACD.macd{}{}".format(pipe,candle),
						"MACD.signal{}{}".format(pipe,candle),
						"Rec.Stoch.RSI{}{}".format(pipe,candle),
						"Stoch.RSI.K{}{}".format(pipe,candle),
						"Rec.WR{}{}".format(pipe,candle),
						"W.R{}{}".format(pipe,candle),
						"Rec.BBPower{}{}".format(pipe,candle),
						"BBPower{}{}".format(pipe,candle),
						"Rec.UO{}{}".format(pipe,candle),
						"UO{}{}".format(pipe,candle),
						"EMA10{}{}".format(pipe,candle),
						"close{}{}".format(pipe,candle),
						"SMA10{}{}".format(pipe,candle),
						"EMA20{}{}".format(pipe,candle),
						"SMA20{}{}".format(pipe,candle),
						"EMA30{}{}".format(pipe,candle),
						"SMA30{}{}".format(pipe,candle),
						"EMA50{}{}".format(pipe,candle),
						"SMA50{}{}".format(pipe,candle),
						"EMA100{}{}".format(pipe,candle),
						"SMA100{}{}".format(pipe,candle),
						"EMA200{}{}".format(pipe,candle),
						"SMA200{}{}".format(pipe,candle),
						"Rec.Ichimoku{}{}".format(pipe,candle),
						"Ichimoku.BLine{}{}".format(pipe,candle),
						"Rec.VWMA{}{}".format(pipe,candle),
						"VWMA{}{}".format(pipe,candle),
						"Rec.HullMA9{}{}".format(pipe,candle),
						"HullMA9{}{}".format(pipe,candle),
						"Pivot.M.Classic.S3{}{}".format(pipe,candle),
						"Pivot.M.Classic.S2{}{}".format(pipe,candle),
						"Pivot.M.Classic.S1{}{}".format(pipe,candle),
						"Pivot.M.Classic.Middle{}{}".format(pipe,candle),
						"Pivot.M.Classic.R1{}{}".format(pipe,candle),
						"Pivot.M.Classic.R2{}{}".format(pipe,candle),
						"Pivot.M.Classic.R3{}{}".format(pipe,candle),
						"Pivot.M.Fibonacci.S3{}{}".format(pipe,candle),
						"Pivot.M.Fibonacci.S2{}{}".format(pipe,candle),
						"Pivot.M.Fibonacci.S1{}{}".format(pipe,candle),
						"Pivot.M.Fibonacci.Middle{}{}".format(pipe,candle),
						"Pivot.M.Fibonacci.R1{}{}".format(pipe,candle),
						"Pivot.M.Fibonacci.R2{}{}".format(pipe,candle),
						"Pivot.M.Fibonacci.R3{}{}".format(pipe,candle),
						"Pivot.M.Camarilla.S3{}{}".format(pipe,candle),
						"Pivot.M.Camarilla.S2{}{}".format(pipe,candle),
						"Pivot.M.Camarilla.S1{}{}".format(pipe,candle),
						"Pivot.M.Camarilla.Middle{}{}".format(pipe,candle),
						"Pivot.M.Camarilla.R1{}{}".format(pipe,candle),
						"Pivot.M.Camarilla.R2{}{}".format(pipe,candle),
						"Pivot.M.Camarilla.R3{}{}".format(pipe,candle),
						"Pivot.M.Woodie.S3{}{}".format(pipe,candle),
						"Pivot.M.Woodie.S2{}{}".format(pipe,candle),
						"Pivot.M.Woodie.S1{}{}".format(pipe,candle),
						"Pivot.M.Woodie.Middle{}{}".format(pipe,candle),
						"Pivot.M.Woodie.R1{}{}".format(pipe,candle),
						"Pivot.M.Woodie.R2{}{}".format(pipe,candle),
						"Pivot.M.Woodie.R3{}{}".format(pipe,candle),
						"Pivot.M.Demark.S1{}{}".format(pipe,candle),
						"Pivot.M.Demark.Middle{}{}".format(pipe,candle),
						"Pivot.M.Demark.R1{}{}".format(pipe,candle)
					]
				}
	resp = requests.post(url,headers=headers,data=json.dumps(payload)).json()
	#signal = resp["data"][0]["d"][1]
	signal = resp["data"][0]["d"]
	return signal

def get_TVsignal(market, exchange):
	url = "https://scanner.tradingview.com/america/scan"
#exchange = "NYSE"
#market = "CODA"
#exchange = "NASDAQ"
#candle = 60
#pipe = '|'
#For 1 Day use the below Candle and pipe parameters
	candle = ''
	pipe = ''

	signal = get_signal(market, pipe, candle, exchange, url)
	return signal
	#return (mlog(market, signal))
    #(-1,-0.5) = strong sell. (-0.5,0) = sell. (0,0.5) = buy. (0.5,1)=strong buy.