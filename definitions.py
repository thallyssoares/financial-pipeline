from dagster import Definitions

from assets.gecko_data import price_df
from resources import BqResource, CoingeckoResource

defs = Definitions(
    assets=[price_df], resources={"bq": BqResource(), "gecko": CoingeckoResource()}
)
