from pipeline_live.engine import LivePipelineEngine
from pipeline_live.data.sources.iex import list_symbols
from pipeline_live.data.iex.pricing import USEquityPricing
from pipeline_live.data.iex.fundamentals import IEXKeyStats, IEXCompany
from pipeline_live.data.iex.factors import AverageDollarVolume, SimpleMovingAverage
from pipeline_live.data.polygon.filters  import IsPrimaryShareEmulation as IsPrimaryShare
from zipline.pipeline import Pipeline
import numpy as np

def make_pipeline_base():
    
    eng = LivePipelineEngine(list_symbols)
    top5 = AverageDollarVolume(window_length=20).top(5)
    
    pipe = Pipeline({
        'close': USEquityPricing.close.latest,
        'marketcap': IEXKeyStats.marketcap.latest,
    }, screen=top5)

    df = eng.run_pipeline(pipe)
    return (df)

def make_pipeline(MaxCandidates=25,acash=250.0):
    
    LowVar=75
    HighVar=100

    m = (
    IsPrimaryShare()                                         # primary_share   4160 with this, 7461 without it
    & ~IEXCompany.exchange    .latest.startswith('OTC')         # not_otc
    & ~IEXCompany.symbol      .latest.endswith('.WI')           # not_wi
    & ~IEXCompany.companyName .latest.matches('.* L[. ]?P.?$')  # not_lp_name
    &  IEXKeyStats.marketcap  .latest.notnull()                 # has market_cap
    )

    eng = LivePipelineEngine(list_symbols)
    base_universe = AverageDollarVolume(window_length=20, mask=m).percentile_between(LowVar,HighVar)
    ShortAvg = SimpleMovingAverage(inputs=[USEquityPricing.close],window_length=20,mask=base_universe)
    LongAvg = SimpleMovingAverage(inputs=[USEquityPricing.close],window_length=200,mask=base_universe)

    percent_difference = (ShortAvg - LongAvg) / LongAvg
    #portfolio_ready = percent_difference *
    # Filter to select securities to long.
    stocks_best = percent_difference.top(MaxCandidates)
    
    securities_to_trade = (stocks_best)
    port_value = ShortAvg <= float(acash)
    pipe = Pipeline({
        'close': USEquityPricing.close.latest,
        'marketcap': IEXKeyStats.marketcap.latest,
        'stocks_best': stocks_best,
    }, screen=securities_to_trade & port_value)

    df = eng.run_pipeline(pipe)
    return (df)

'''
if __name__ == "__main__":
    MaxCandidates = 25
    acash = 50
    my_pipe = make_pipeline(MaxCandidates,acash)
    print("pipeline stocks - {}".format(my_pipe))
'''