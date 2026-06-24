from datetime import datetime

import pandas as pd
from dagster import asset

from resources import BqResource, CoingeckoResource


@asset()
def price_df(gecko: CoingeckoResource, bq: BqResource) -> None:
    client = gecko.get_client()

    coins = ["bitcoin", "solana", "ethereum"]

    rows = []

    for c in coins:
        response = client.coins.get_id(c)

        json_payload = response.model_dump_json(indent=2)

        rows.append(
            {
                "ingestion_timestamp": datetime.now().isoformat(),
                "raw_payload": json_payload,
            }
        )

    coin_df = pd.DataFrame(rows)
    bq.save_data(coin_df, dataset="bronze", table="coin_data")
