from dagster import define_asset_job

# define los jobs por separados para tener mayor control

job_all_assets = define_asset_job(name="job_all_assets", selection="*")

job_centro = define_asset_job(
    name="job_centro",
    selection=[
        "raw_data_centro",
        "clean_data_centro",
        "clean_csv_data_centro",
        "mongodb_data_centro",
        "postgres_data_centro",
        "duckdb_data_centro",
    ],  # Selecciona solo los assets relacionados con centro
    description="job para procesar datos de centro",
    
)

job_ofertas = define_asset_job(
    name="job_ofertas",
    selection=[
        "raw_data_ofertas",
        "clean_data_ofertas",
        "clean_csv_data_ofertas",
        "mongodb_data_ofertas",
        "postgres_data_ofertas",
        "duckdb_data_ofertas",
    ],  # Selecciona solo los assets relacionados con ofertas
    description="job para procesar datos de ofertas",
)

job_regional = define_asset_job(
    name="job_regional",
    selection=[
        "raw_data_regional",
        "clean_data_regional",
        "clean_csv_data_regional",
        "mongodb_data_regional",
        "postgres_data_regional",
        "duckdb_data_regional",
    ],  # Selecciona solo los assets relacionados con regional
    description="job para procesar datos de regional",
)

# lista para importar fácilmente en definitions.py
all_jobs = [job_all_assets, job_centro, job_ofertas, job_regional]
