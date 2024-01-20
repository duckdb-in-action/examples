from dagster import Definitions, load_assets_from_modules
from dagster_duckdb import DuckDBResource
import os

from dagster import (
    AssetSelection,
    ScheduleDefinition,
    Definitions,
    define_asset_job,
    load_assets_from_modules,
)

from . import assets

import dotenv
dotenv.load_dotenv()

all_assets = load_assets_from_modules([assets])

# Addition: define a job that will materialize the assets
atp_job = define_asset_job("atp_job", selection=AssetSelection.all())

atp_schedule = ScheduleDefinition(
    job=atp_job,
    cron_schedule="0 * * * *",  # every hour
)

mduck_token = os.getenv("MD_TOKEN")
defs = Definitions(
    assets=all_assets,
    jobs=[atp_job],
    resources={"duckdb": DuckDBResource(
        # database="atp.duckdb",
        database=f"md:md_atp_db?motherduck_token={mduck_token}", schema="main"
    )},
    schedules=[atp_schedule],
)
