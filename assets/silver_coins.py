import json
from typing import Any

import pandas as pd
from dagster import asset

from resources import BqResource, get_dagster_logger

logger = get_dagster_logger()

def parse_coin_payload(raw_payload: str, ingestion_timestamp: str) -> dict[str, Any]:
    """
    Recebe a string JSON do payload bruto e o timestamp da extração,
    faz o parse do JSON e retorna um dicionário estruturado e limpo.
    """
    payload = json.loads(raw_payload)

    market_data = payload.get("market_data", {})

    current_price = market_data.get("current_price", {})
    market_cap = market_data.get("market_cap", {})
    total_volume = market_data.get("total_volume", {})

    return {
        "coin_id": payload.get("id"),
        "symbol": payload.get("symbol"),
        "name": payload.get("name"),
        "price_usd": float(current_price.get("usd")) if current_price.get("usd") is not None else None,
        "market_cap_usd": float(market_cap.get("usd")) if market_cap.get("usd") is not None else None,
        "total_volume_usd": float(total_volume.get("usd")) if total_volume.get("usd") is not None else None,
        "price_change_percentage_24h": float(market_data.get("price_change_percentage_24h"))
        if market_data.get("price_change_percentage_24h") is not None else None,
        "extracted_at": pd.to_datetime(ingestion_timestamp)
    }


def transform_bronze_coins_to_silver(raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Recebe o DataFrame com dados brutos da Bronze e aplica a lógica de parsing
    em cada registro, retornando um novo DataFrame estruturado para a Silver.
    """
    parsed_rows = []

    for idx, row in raw_df.iterrows():
        try:
            parsed_data = parse_coin_payload(row["raw_payload"], row["ingestion_timestamp"])
            parsed_rows.append(parsed_data)
        except Exception as e:
            logger.error(f"Erro ao processar linha de índice {idx}. Detalhe: {str(e)}")
            continue

    return pd.DataFrame(parsed_rows)


@asset(deps=["raw_coins_data"])
def silver_coins(bq: BqResource) -> None:
    """
    Asset da camada Silver.
    Orquestra a leitura da tabela bronze.coin_data, aciona a transformação pura
    e grava os dados de volta no BigQuery (sobrescrevendo de forma idempotente).
    """
    logger.info("Iniciando a leitura dos dados brutos da Bronze...")

    query_bronze = "SELECT ingestion_timestamp, raw_payload FROM bronze.coin_data"
    raw_df = bq.read_data(query_bronze)

    if raw_df.empty:
        logger.warning("Nenhum dado encontrado na Bronze. Finalizando execução.")
        return

    logger.info(f"Encontrados {len(raw_df)} registros. Iniciando transformação...")

    silver_df = transform_bronze_coins_to_silver(raw_df)
    logger.info(f"Transformação concluída. Total de linhas geradas: {len(silver_df)}")

    bq.save_data(silver_df, dataset="silver", table="coins", write_disposition="WRITE_TRUNCATE")
    logger.info("Carga na camada Silver concluída com sucesso.")

