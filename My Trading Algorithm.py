import quantopian.algorithm as algo
from quantopian.pipeline import Pipeline
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.filters import QTradableStocksUS
from quantopian.pipeline.factors import Returns
import quantopian.optimize as opt

# (minimo crecimiento, máximo crecimiento, porcentaje a invertir)
STRONG = (0.1, 10, 0.3)
MEDIUM = (0, 0.05, 0.4)
LOW = (-0.01, 0.03, 0.3)


def initialize(context):
    
    i_percent_to_invert = 2
    
    algo.schedule_function(
        getDefaultRun(STRONG[i_percent_to_invert], 'strong'),
        algo.date_rules.week_start(days_offset=0),
        algo.time_rules.market_open(hours=1),
    )
    
    algo.schedule_function(
        getDefaultRun(MEDIUM[i_percent_to_invert], 'medium'),
        algo.date_rules.week_start(days_offset=0),
        algo.time_rules.market_open(hours=1, minutes= 30),
    )
    
    algo.schedule_function(
        getDefaultRun(LOW[i_percent_to_invert], 'low'),
        algo.date_rules.week_start(days_offset=0),
        algo.time_rules.market_open(hours=2),
    )


    algo.attach_pipeline(getDefaultPipe(STRONG[0], STRONG[1]), 'strong_pipe')
    algo.attach_pipeline(getDefaultPipe(MEDIUM[0], MEDIUM[1]), 'medium_pipe')
    algo.attach_pipeline(getDefaultPipe(LOW[0], LOW[1]), 'low_pipe')



def getDefaultPipe(min_grow, max_grow):
    returns_7 = Returns(
        inputs=[USEquityPricing.close],
        window_length=7
    ).zscore()

    returns_30 = Returns(
        inputs=[USEquityPricing.close],
        window_length=30
    ).zscore()

    returns = (returns_7 - returns_30)

    grow = returns > min_grow and returns < max_grow and returns_7 > 0 and returns.notnull()
    
    return Pipeline(
        columns={
            'returns': returns
        },
        screen=grow
    )
    
def before_trading_start(context, data):
   
    context.strong = algo.pipeline_output('strong_pipe')
    context.medium = algo.pipeline_output('medium_pipe')
    context.low = algo.pipeline_output('low_pipe')


def getDefaultRun(max_invert, pipe):
    def run(context, data):
        objective = opt.MaximizeAlpha(context[pipe]['returns'])
        constraints = [
            opt.MaxGrossExposure(max_invert),
            opt.DollarNeutral()
        ]

        algo.order_optimal_portfolio(objective, constraints)
    
    return run