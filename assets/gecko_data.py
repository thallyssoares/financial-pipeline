from datetime import datetime

import pandas as pd
from dagster import asset

from resources import BqResource, CoingeckoResource, get_dagster_logger

logger = get_dagster_logger()


@asset()
def price_df(gecko: CoingeckoResource, bq: BqResource) -> None:
    client = gecko.get_client()

    coins = ["bitcoin", "solana", "ethereum"]

    rows = []
    logger.info(f"Iniciando a extração de dados do Coingecko para {len(coins)} moedas")

    for c in coins:
        logger.info(f"Buscando dados para a moeda: {c}")
        try:
            response = client.coins.get_id(c)

            json_payload = response.model_dump_json(indent=2)

            rows.append(
                {
                    "ingestion_timestamp": datetime.now().isoformat(),
                    "raw_payload": json_payload,
                }
            )
            logger.info(f"Dados da moeda {c} extraidos com sucesso")
        except Exception as e:
            logger.error(
                f"Não foi possivel extrair os dados da moeda {c}. Erro: {str(e)}"
            )
            raise e
    coin_df = pd.DataFrame(rows)
    logger.info(
        f"Dataframe consolidado com sucesso. Total de registros: {len(coin_df)}"
    )

    bq.save_data(coin_df, dataset="bronze", table="coin_data")
