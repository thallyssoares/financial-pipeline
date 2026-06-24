import pandas as pd
from coingecko_sdk import Coingecko
from dagster import ConfigurableResource, EnvVar
from google.cloud import bigquery as bq


class BqResource(ConfigurableResource):
    project_id: str = EnvVar("BQ_PROJECT_ID")

    def get_client(self) -> bq.Client:
        return bq.Client(project=self.project_id)

    def save_data(self, df: pd.DataFrame, dataset: str, table: str):
        table_id = f"{dataset}.{table}"
        client = self.get_client()

        client.create_dataset(dataset, exists_ok=True)

        job_config = bq.LoadJobConfig(write_disposition="WRITE_APPEND")

        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)

        job.result()


class CoingeckoResource(ConfigurableResource):
    api_key: str = EnvVar("GECKO_API_KEY")

    def get_client(self):
        client = Coingecko(demo_api_key=self.api_key, environment="demo")
        return client
