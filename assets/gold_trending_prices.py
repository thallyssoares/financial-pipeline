import pandas as pd
from dagster import asset
from resources import BqResource, get_dagster_logger

logger = get_dagster_logger()

def enrich_trending_with_btc_prices(df_trending: pd.DataFrame, df_coins: pd.DataFrame) -> pd.DataFrame:
    """
    Função pura que recebe a tabela de trending e a tabela de moedas da Silver,
    isola o histórico de preços do Bitcoin, alinha os registros temporais usando merge_asof
    e calcula o preço estimado em USD para cada trending coin no respectivo timestamp.
    """
    df_btc = df_coins[df_coins["coin_id"] == "bitcoin"][["price_usd", "extracted_at"]].copy()
    df_btc = df_btc.rename(columns={"price_usd": "btc_price_usd"})
    
    if df_btc.empty:
        raise ValueError("Dados de preço do Bitcoin ausentes na tabela silver.coins. Impossível prosseguir.")
        
    df_trending["extracted_at"] = pd.to_datetime(df_trending["extracted_at"])
    df_btc["extracted_at"] = pd.to_datetime(df_btc["extracted_at"])
    
    df_trending = df_trending.sort_values("extracted_at")
    df_btc = df_btc.sort_values("extracted_at")
    
    df_enriched = pd.merge_asof(
        df_trending,
        df_btc,
        on="extracted_at",
        direction="nearest",
        tolerance=pd.Timedelta("1h")
    )
    
    df_enriched["price_usd_estimated"] = df_enriched["price_btc"] * df_enriched["btc_price_usd"]
    
    return df_enriched[[
        "coin_id",
        "name",
        "symbol",
        "trending_rank",
        "market_cap_rank",
        "price_btc",
        "btc_price_usd",
        "price_usd_estimated",
        "extracted_at"
    ]]


@asset(deps=["silver_coins", "silver_trending"])
def gold_trending_prices(bq: BqResource) -> None:
    """
    Asset da camada Gold.
    Orquestra o fluxo de leitura das tabelas Silver, aciona a função de cruzamento temporal
    e grava a tabela analítica 'gold.trending_prices' no BigQuery.
    """
    logger.info("Iniciando leitura das tabelas estruturadas da camada Silver...")
    
    df_trending = bq.read_data("SELECT * FROM silver.trending")
    df_coins = bq.read_data("SELECT * FROM silver.coins")
    
    if df_trending.empty or df_coins.empty:
        logger.warning("Uma ou mais tabelas Silver estão vazias. Abortando criação da Gold.")
        return
        
    logger.info(f"Dados lidos (Trending: {len(df_trending)} linhas, Coins: {len(df_coins)} linhas). Iniciando enriquecimento...")
    
    gold_df = enrich_trending_with_btc_prices(df_trending, df_coins)
    logger.info(f"Enriquecimento finalizado. Dataframe Gold gerado com {len(gold_df)} registros.")
    
    bq.save_data(gold_df, dataset="gold", table="trending_prices", write_disposition="WRITE_TRUNCATE")
    logger.info("Carga na camada Gold concluída com sucesso. Tabela pronta para o Looker Studio.")

