from dagster import Definitions

from resources import save_csv

all_assets = []

defs = Definitions(assets=all_assets, resources={"io_manager": save_csv})
