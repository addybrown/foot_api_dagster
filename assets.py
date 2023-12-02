import sys
import time
import pandas as pd

from datetime import datetime, timedelta
from dagster import asset

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


@asset
def update_schedule_table():
    update_schedule_table()


@asset
def update_match_details(update_schedule_table: str):
    if update_schedule_table == "Done":
        update_match_details_table()


@asset
def update_match_odds(update_schedule_table: str):
    if update_schedule_table == "Done":
        update_match_odds_table()


@asset
def update_match_shotmap(update_schedule_table: str):
    if update_schedule_table == "Done":
        update_match_shotmap_table()


@asset
def update_match_incidents(update_schedule_table: str):
    if update_schedule_table == "Done":
        update_match_incidents_table()


@asset
def update_match_lineup(update_schedule_table: str):
    if update_schedule_table == "Done":
        update_match_lineup_and_player_statistics()


@asset
def update_match_statistics(update_schedule_table: str):
    if update_schedule_table == "Done":
        update_match_statistics_table()
