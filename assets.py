import sys
import time
import pandas as pd

from datetime import datetime, timedelta
from dagster import asset, FreshnessPolicy, cron_schedule


from dotenv import load_dotenv
from foot_api_data_pipeline.schedule import update_schedule_table
from foot_api_data_pipeline.pipeline_services import get_schedule

# Imports for Match Updater Files
from foot_api_data_pipeline.match_details import update_match_details_table
from foot_api_data_pipeline.match_odds import update_match_odds_table
from foot_api_data_pipeline.match_shotmap import update_match_shotmap_table
from foot_api_data_pipeline.match_incidents import update_match_incidents_table
from foot_api_data_pipeline.match_lineup import (
    update_match_lineup_and_player_statistics,
)
from foot_api_data_pipeline.match_statistics import update_match_statistics_table


from foot_api_data_pipeline.match_lineup import (
    update_match_lineup_and_player_statistics,
)
from foot_api_harvesting.utils import FootApiHarvester
from services.sql_services import (
    bulk_upsert_write_sql,
    create_session,
    read_sql,
    write_sql,
)

from foot_api_data_pipeline.variables import (
    MATCH_DETAILS_DATAFRAME,
    PLAYER_TABLE_VARIABLES,
    RELEVANT_LEAGUES,
)


api_client = FootApiHarvester()

load_dotenv()


@asset(
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60), cron_schedule="0 * * * *"
)
def update_schedule():
    update_schedule_table()
    return "Done"


@asset(
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60), cron_schedule="0 * * * *"
)
def update_match_details(update_schedule: str):
    if update_schedule == "Done":
        update_match_details_table()


@asset(
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60), cron_schedule="0 * * * *"
)
def update_match_odds(update_schedule: str):
    if update_schedule == "Done":
        update_match_odds_table()


@asset(
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60), cron_schedule="0 * * * *"
)
def update_match_shotmap(update_schedule: str):
    if update_schedule == "Done":
        update_match_shotmap_table()


@asset(
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60), cron_schedule="0 * * * *"
)
def update_match_incidents(update_schedule: str):
    if update_schedule == "Done":
        update_match_incidents_table()


@asset(
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60), cron_schedule="0 * * * *"
)
def update_match_lineup(update_schedule: str):
    if update_schedule == "Done":
        update_match_lineup_and_player_statistics()


@asset(
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60), cron_schedule="0 * * * *"
)
def update_match_statistics(update_schedule: str):
    if update_schedule == "Done":
        update_match_statistics_table()
