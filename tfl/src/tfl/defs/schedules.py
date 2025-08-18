from typing import Union

import dagster as dg

crowding_data_schedule = dg.ScheduleDefinition(
    name = 'crowding_data_schedule',
    cron_schedule = '0/10 * * * *',
    job_name='update_crowding_data'
)

crowding_figures_schedule = dg.ScheduleDefinition(
    name = 'crowding_figures_schedule',
    cron_schedule = '0 9 * * *',
    job_name='update_crowding_figures'
)