from dagster import Definitions, load_assets_from_package_module
from jobs.jobs import all_jobs
from schedules.schedules import all_schedules
import assets

# Import all assets, jobs, and schedules packages
all_assets = load_assets_from_package_module(assets)

# Import dagster definitions
defs = Definitions(
    assets=all_assets,
    jobs=all_jobs,
    schedules=all_schedules
)

