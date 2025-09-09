from dagster import ScheduleDefinition

# ✅ Referenciar por NOMBRE, no importar el objeto
minute_etl_schedule = ScheduleDefinition(
    job_name="etl_assets_job",  # ← por NOMBRE
    cron_schedule="* * * * *",
    name="minute_etl_schedule"
)

hourly_etl_schedule = ScheduleDefinition(
    job_name="etl_assets_job",  # ← por NOMBRE
    cron_schedule="0 * * * *",
    name="hourly_etl_schedule"
)