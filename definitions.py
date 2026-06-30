from dagster import Definitions, JobDefinition, ScheduleDefinition, define_asset_job

from assets.raw_coins_data import raw_coins_data
from assets.raw_trending_data import raw_trending_data
from assets.silver_coins import silver_coins
from assets.silver_trending import silver_trending
from assets.gold_trending_prices import gold_trending_prices, gold_trending_prices_check

from resources import BqResource, CoingeckoResource

assets = [
    raw_coins_data,
    raw_trending_data,
    silver_coins,
    silver_trending,
    gold_trending_prices,
]

daily_refresh_job: JobDefinition = define_asset_job(
    name="daily_refresh",
    selection=assets,
)

daily_schedule: ScheduleDefinition = ScheduleDefinition(
    job=daily_refresh_job,
    cron_schedule="0 8 * * *",
)

defs = Definitions(
    assets=assets,
    resources={
        "bq": BqResource(),
        "gecko": CoingeckoResource(),
    },
    schedules=[daily_schedule],
    asset_checks=[gold_trending_prices_check],
)


