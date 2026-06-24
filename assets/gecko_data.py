from datetime import datetime

import pandas as pd
from dagster import asset

from resources import BqResource, CoingeckoResource


@asset()
def price_df(gecko: CoingeckoResource, bq: BqResource) -> None:
    client = gecko.get_client()
    response = client.coins.get_id("bitcoin")

    json_payload = response.model_dump_json(indent=2)

    coin_df = pd.DataFrame(
        [
            {
                "ingestion_timestamp": datetime.now().isoformat(),
                "raw_payload": json_payload,
            }
        ]
    )
