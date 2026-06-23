from dagster import Definitions, EnvVar

from resources import BqResource

all_assets = []

defs = Definitions(assets=all_assets, resources={"bq": BqResource()})
