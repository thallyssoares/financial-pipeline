import pandas as pd
from coingecko_sdk import Coingecko
from dagster import ConfigurableResource, EnvVar, get_dagster_logger
from google.cloud import bigquery as bq

logger = get_dagster_logger()

class BqResource(ConfigurableResource):
    project_id: str = EnvVar("BQ_PROJECT_ID")

    def get_client(self) -> bq.Client:
        return bq.Client(project=self.project_id)

    def save_data(self, df: pd.DataFrame, dataset: str, table: str, write_disposition: str = "WRITE_APPEND") -> None:
        table_id = f"{dataset}.{table}"
        client = self.get_client()

        logger.info(f"Verificando se o dataset: {dataset} existe")
        client.create_dataset(dataset, exists_ok=True)
        logger.info(
            f"Iniciando o upload de {len(df)} linhas para a tabela {table}-{table_id}"
            f" (disposição: {write_disposition})"
        )

        try:
            job_config = bq.LoadJobConfig(write_disposition=write_disposition)
            job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()

            logger.info(f"Upload realizado com sucesso. Dados inseridos na tabela {table_id}")
        except Exception as e:
            logger.error(f"Falha no upload de dados na tabela {table_id}. Erro: {str(e)}")
            raise e

    def read_data(self, query: str) -> pd.DataFrame:
        """
        Executa uma consulta SQL no BigQuery e retorna os resultados como um DataFrame do Pandas.
        Isso nos permite ler dados das tabelas da camada Bronze para estruturá-los na Silver,
        e ler da Silver para gerar a Gold.
        """
        client = self.get_client()
        logger.info(f"Executando consulta SQL no BigQuery: {query}")
        try:
            df = client.query(query).to_dataframe()
            logger.info(f"Consulta finalizada. Retornados {len(df)} registros.")
            return df
        except Exception as e:
            logger.error(f"Erro ao executar consulta SQL: {str(e)}")
            raise e


class CoingeckoResource(ConfigurableResource):
    api_key: str = EnvVar("GECKO_API_KEY")

    def get_client(self) -> Coingecko:
        return Coingecko(demo_api_key=self.api_key, environment="demo")
