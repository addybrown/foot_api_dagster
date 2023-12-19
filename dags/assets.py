import sys
import time
import pandas as pd

from datetime import datetime, timedelta
from dagster import asset

from dotenv import load_dotenv
from foot_api_data_pipeline.schedule import get_schedule_df
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
from foot_api_data_pipeline.players import update_player_table

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
def generate_date_list(context):
    today = datetime.now().date()
    three_days_later = today + timedelta(days=3)

    date_list = [
        str(today + timedelta(days=x))
        for x in range((three_days_later - today).days + 1)
    ]

    context.log.info(f"Generated date list: {date_list}")
    return date_list


@asset
def get_schedule_dfs(generate_date_list: list):
    all_schedules = []
    for date in generate_date_list:
        date_format = "%Y-%m-%d"
        date_object = datetime.strptime(date, date_format)
        day = str(date_object.day)
        month = str(date_object.month)
        year = str(date_object.year)
        schedule_response_json = api_client.get_schedule_response_json(
            day=day, month=month, year=year
        )

        schedule_df = get_schedule_df(
            day=day,
            month=month,
            year=year,
            schedule_response_json=schedule_response_json,
        )

        schedule_df["created_on"] = datetime.utcnow()

        all_schedules.append(schedule_df)

    return pd.concat(all_schedules)


@asset
def update_schedule_table(get_schedule_dfs):
    session = create_session()
    bulk_upsert_write_sql(
        df=get_schedule_dfs,
        dbtable="schedule",
        dbschema="football_data",
        session=session,
    )
    return "Done"


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


@asset
def update_player(update_schedule_table: str):
    if update_schedule_table == "Done":
        update_player_table()
