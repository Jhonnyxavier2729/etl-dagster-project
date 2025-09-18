from dagster import ScheduleDefinition
from etl_dagster.jobs.jobs import job_all_assets, job_centro, job_ofertas, job_regional

all_schedules = [
    ScheduleDefinition(
        job=job_all_assets,
        cron_schedule="* * * * *",  # cada minuto
    ),
    ScheduleDefinition(
        job=job_centro,
        cron_schedule="*/5 * * * *",  # cada 5 minutos
    ),
    ScheduleDefinition(
        job=job_ofertas,
        cron_schedule="*/15 * * * *",  # cada 15 minutos
    ),
    ScheduleDefinition(
        job=job_regional,
        cron_schedule="* * * * *",  # cada minuto
    ),
]
