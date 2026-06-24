import pandas as pd
from coingecko_sdk import Coingecko
from dagster import ConfigurableResource, EnvVar, get_dagster_logger
from google.cloud import bigquery as bq

logger = get_dagster_logger()

class BqResource(ConfigurableResource):
    project_id: str = EnvVar("BQ_PROJECT_ID")

    def get_client(self) -> bq.Client:
        return bq.Client(project=self.project_id)

    def save_data(self, df: pd.DataFrame, dataset: str, table: str):
        table_id = f"{dataset}.{table}"
        client = self.get_client()

        logger.info(f"Verificando se o dataset: {dataset} existe")
        client.create_dataset(dataset, exists_ok=True)
        logger.info(f"Iniciando o upload de {len(df)} linhas para a tabela {table}-{table_id}")

        try:
            job_config = bq.LoadJobConfig(write_disposition="WRITE_APPEND")

            job = client.load_table_from_dataframe(df, table_id, job_config=job_config)

            job.result()

            logger.info(f"Upload realizado com sucesso. Dados inseridos na tabela {table_id}")
        except Exception as e:
            logger.error(f"Falha no upload de dados na tabela {table_id}. Erro: {str(e)}")
            raise e

class CoingeckoResource(ConfigurableResource):
    api_key: str = EnvVar("GECKO_API_KEY")

    def get_client(self):
        client = Coingecko(demo_api_key=self.api_key, environment="demo")
        return client
