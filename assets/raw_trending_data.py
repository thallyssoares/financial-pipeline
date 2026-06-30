from datetime import datetime

import pandas as pd
from dagster import asset

from resources import BqResource, CoingeckoResource, get_dagster_logger

logger = get_dagster_logger()

@asset
def raw_trending_data(gecko: CoingeckoResource, bq: BqResource) -> None:
    """
    Asset da camada Bronze.
    Consome o endpoint de trending coins (moedas em alta) do Coingecko
    e salva o payload JSON bruto na tabela 'trending_data' no dataset 'bronze' do BigQuery.
    """

    client = gecko.get_client()

    logger.info("Iniciando a extração das trending coins no Coingecko...")

    try:
        response = client.search.trending.get()

        json_payload = response.model_dump_json(indent=2)

        rows = [
            {
                "ingestion_timestamp": datetime.now().isoformat(),
                "raw_payload": json_payload,
            }
        ]

        trending_df = pd.DataFrame(rows)
        logger.info("Dados de trending extraídos com sucesso. Consolidando DataFrame...")

        bq.save_data(trending_df, dataset="bronze", table="trending_data")
        logger.info("Carga na tabela bronze.trending_data finalizada com sucesso.")

    except Exception as e:
        logger.error(f"Falha ao extrair moedas em alta (trending) do Coingecko. Erro: {str(e)}")
        raise e

