from dagster import Definitions, load_assets_from_package_module
from etl_dagster.jobs.jobs import all_jobs
from etl_dagster.schedules.schedules import all_schedules
from etl_dagster import assets

# Import all assets, jobs, and schedules packages
all_assets = load_assets_from_package_module(assets)

# Import dagster definitions
defs = Definitions(
    assets=all_assets,
    jobs=all_jobs,
    schedules=all_schedules
)

