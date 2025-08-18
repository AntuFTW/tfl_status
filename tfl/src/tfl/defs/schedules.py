from typing import Union

import dagster as dg

crowding_data_schedule = dg.ScheduleDefinition(
    name = 'crowding_data_schedule',
    cron_schedule = '0/10 * * * *',
    job_name='update_crowding_data'
)