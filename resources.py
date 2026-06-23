import pandas as pd
from dagster import ConfigurableResource, EnvVar
from google.cloud import bigquery as bq


class BqResource(ConfigurableResource):
    project_id: str = EnvVar("BQ_PROJECT_ID")

    def get_client(self) -> bq.Client:
        return bq.Client(project=self.project_id)

    def save_data(self, df: pd.DataFrame, dataset: str, table: str):
        table_id = f"{self.project_id}.{dataset}.{table}"
        df.to_gbq(table_id, project_id=self.project_id, if_exists="replace")
