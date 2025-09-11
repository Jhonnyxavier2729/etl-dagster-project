from dagster import Definitions, load_assets_from_package_module
import assets
from jobs.jobs import all_jobs
from schedules.schedules import all_schedules

#se importa todo el paquete de assets, jobs y schedules
all_assets = load_assets_from_package_module(assets)

#se importa las definiciones de dagster
defs = Definitions(
    assets=all_assets,
    jobs=all_jobs,
    schedules=all_schedules
)

