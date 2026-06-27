from datetime import datetime
import pandas as pd
from dagster import asset
from resources import BqResource, CoingeckoResource, get_dagster_logger

logger = get_dagster_logger()

@asset
def raw_coins_data(gecko: CoingeckoResource, bq: BqResource) -> None:
    """
    Asset da camada Bronze.
    Extrai as informações de preços atuais e metadados brutos das 3 moedas de interesse
    e as salva exatamente como recebidas (formato JSON raw) no BigQuery.
    """
    
    client = gecko.get_client()

    coins = ["bitcoin", "solana", "ethereum"]
    rows = []
    
    logger.info(f"Iniciando a extração de dados do Coingecko para as moedas: {coins}")

    for c in coins:
        logger.info(f"Buscando dados brutos da API Coingecko para: {c}")
        try:
            response = client.coins.get_id(c)

            json_payload = response.model_dump_json(indent=2)

            rows.append(
                {
                    "ingestion_timestamp": datetime.now().isoformat(),
                    "raw_payload": json_payload,
                }
            )
            logger.info(f"Dados brutos da moeda '{c}' extraídos com sucesso.")
            
        except Exception as e:
            logger.error(f"Falha ao extrair dados para a moeda '{c}'. Erro: {str(e)}")
            raise e

    coin_df = pd.DataFrame(rows)
    logger.info(f"Dataframe de dados brutos consolidado com sucesso. Total de linhas: {len(coin_df)}")

    bq.save_data(coin_df, dataset="bronze", table="coin_data")

