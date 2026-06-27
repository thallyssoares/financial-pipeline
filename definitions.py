from dagster import Definitions

from assets.raw_coins_data import raw_coins_data
from assets.raw_trending_data import raw_trending_data
from assets.silver_coins import silver_coins
from assets.silver_trending import silver_trending
from assets.gold_trending_prices import gold_trending_prices

from resources import BqResource, CoingeckoResource

defs = Definitions(
    assets=[
        raw_coins_data,
        raw_trending_data,
        silver_coins,
        silver_trending,
        gold_trending_prices
    ],
    resources={
        "bq": BqResource(),
        "gecko": CoingeckoResource()
    }
)


