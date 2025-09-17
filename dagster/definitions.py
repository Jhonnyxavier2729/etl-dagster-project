from dagster import Definitions, load_assets_from_modules
from assets_csv import centro, ofertas, regional
from checks.checks_centro import all_asset_checks
from jobs.jobs import all_jobs
from schedules.schedules import all_schedules



# Cargar assets y checks
all_assets = load_assets_from_modules([centro, ofertas, regional])

# Definiciones de Dagster
defs = Definitions(
    assets=all_assets, 
    asset_checks=all_asset_checks,
    jobs=all_jobs,
    schedules=all_schedules,
)
