import json
from typing import Any

import pandas as pd
from dagster import asset

from resources import BqResource, get_dagster_logger

logger = get_dagster_logger()

def parse_trending_payload(raw_payload: str, ingestion_timestamp: str) -> list[dict[str, Any]]:
    """
    Faz o parse de um único JSON de trending coins e extrai a lista de moedas
    em alta com seus respectivos rankings no momento da ingestão.
    """
    payload = json.loads(raw_payload)
    coins_list = payload.get("coins", [])

    parsed_items = []

    for rank_index, coin_entry in enumerate(coins_list):
        item = coin_entry.get("item", {})

        parsed_items.append({
            "coin_id": item.get("id"),
            "name": item.get("name"),
            "symbol": item.get("symbol"),
            "market_cap_rank": int(item.get("market_cap_rank")) if item.get("market_cap_rank") is not None else None,
            "trending_rank": rank_index + 1,
            "price_btc": float(item.get("price_btc")) if item.get("price_btc") is not None else None,
            "extracted_at": pd.to_datetime(ingestion_timestamp)
        })

    return parsed_items


def transform_bronze_trending_to_silver(raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Recebe o DataFrame cru da Bronze e converte todos os registros e sub-listas
    em um DataFrame tabular e estruturado.
    """
    all_parsed_rows = []

    for idx, row in raw_df.iterrows():
        try:
            rows = parse_trending_payload(row["raw_payload"], row["ingestion_timestamp"])
            all_parsed_rows.extend(rows)
        except Exception as e:
            logger.error(f"Erro ao parsear snapshot no índice {idx}. Detalhe: {str(e)}")
            continue

    return pd.DataFrame(all_parsed_rows)


@asset(deps=["raw_trending_data"])
def silver_trending(bq: BqResource) -> None:
    """
    Asset da camada Silver.
    Orquestra o carregamento dos dados brutos de trending da Bronze, aciona a transformação,
    e salva o resultado consolidado e limpo na tabela 'silver.trending'.
    """
    logger.info("Iniciando a leitura dos dados brutos de trending da Bronze...")

    query = "SELECT ingestion_timestamp, raw_payload FROM bronze.trending_data"
    raw_df = bq.read_data(query)

    if raw_df.empty:
        logger.warning("Nenhum dado bruto encontrado na Bronze. Finalizando.")
        return

    logger.info(f"Lidos {len(raw_df)} registros brutos. Iniciando transformação...")

    silver_trending_df = transform_bronze_trending_to_silver(raw_df)
    logger.info(f"Transformação concluída. Total de linhas geradas: {len(silver_trending_df)}")

    bq.save_data(silver_trending_df, dataset="silver", table="trending", write_disposition="WRITE_TRUNCATE")
    logger.info("Carga na camada Silver de Trending concluída com sucesso.")

