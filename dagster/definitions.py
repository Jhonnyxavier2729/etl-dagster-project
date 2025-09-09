from dagster import Definitions, load_assets_from_package_module, define_asset_job
import assets
from assets import extract, transform, load_csv, load_duckdb, load_mongodb, load_postgres
from schedules import minute_etl_schedule, hourly_etl_schedule

all_assets = load_assets_from_package_module(assets)

etl_assets_job = define_asset_job(
    name="etl_assets_job",
    selection="*" 
)

defs = Definitions(
    assets=all_assets,
    jobs=[etl_assets_job],
    schedules=[minute_etl_schedule, hourly_etl_schedule]
)