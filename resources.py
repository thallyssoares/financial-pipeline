import pandas as pd
from coingecko_sdk import Coingecko
from dagster import ConfigurableResource, EnvVar
from google.cloud import bigquery as bq


class BqResource(ConfigurableResource):
    project_id: str = EnvVar("BQ_PROJECT_ID")

    def get_client(self) -> bq.Client:
        return bq.Client(project=self.project_id)

    def save_data(self, df: pd.DataFrame, dataset: str, table: str):
        table_id = f"{self.project_id}.{dataset}.{table}"
        df.to_gbq(table_id, project_id=self.project_id, if_exists="replace")


class CoingeckoResource(ConfigurableResource):
    api_key = EnvVar("GECKO_API_KEY")

    def get_client(self):
        client = Coingecko(demo_api_key=self.api_key, environment="demo")
        return client
