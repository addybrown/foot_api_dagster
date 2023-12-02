import pandas as pd
import numpy as np
import time
import os
from collections import ChainMap
from dotenv import load_dotenv

load_dotenv()
all_sports_api_key = os.getenv("all_sports_api_key")

from services.sql_services import (
    read_sql,
    write_sql,
    check_table_exists,
    create_session,
    bulk_upsert_write_sql,
)

from services.pandas_services import append_to_dict_list
from foot_api_harvesting.utils import FootApiHarvester
from foot_api_data_pipeline.variables import (
    MATCH_ODDS_DATAFRAME,
    PLAYER_TABLE_VARIABLES,
    RELEVANT_LEAGUES,
)
from foot_api_data_pipeline.pipeline_services import get_schedule


def update_match_odds_table(
    db_table=None,
    db_schema=None,
    session=None,
    wait_time=5,
    api_key=None,
    date_range=None,
    start_date=None,
    end_date=None,
    country_list=None,
    league_list=None,
    **kwargs,
):
    """

    This function updates the match_details table, for all data that does not currently exist for the match_ids
    in the following three days, if match_id data already exists, it will not update those games.

    Args:
        db_table (str, optional): the database name. Defaults to None.
        db_schema (str, optional): the database schema name. Defaults to None.
        session (sql.alchemy.engine, optional): the database session connection. Defaults to None.
        wait_time (int, optional): the wait_time is defined as the time between api calls. Defaults to 5.
        api_key (str, optional): api_key value. Defaults to None.
        date_range (int, optional): _description_. Defaults to 3.
    """
    if not session:
        session = create_session()

    if not db_table:
        db_table = "match_odds"

    if not db_schema:
        db_schema = "football_data"

    if not api_key:
        api_key = all_sports_api_key

    if start_date:
        if not end_date:
            raise Exception("You need end_date with start_date parameter")

    if not country_list:
        country_list = PLAYER_TABLE_VARIABLES["player_table_country_names"]

    if not league_list:
        league_list = RELEVANT_LEAGUES

    # Creates Default table in case of deletion
    if not check_table_exists(db_table=db_table, db_schema=db_schema, session=session):
        match_odds_df = get_match_odds_df(
            match_id="9576260", db_schema=db_schema, session=session, api_key=api_key
        )
        print(match_odds_df)
        write_sql(
            match_odds_df,
            db_table=db_table,
            db_schema=db_schema,
            session=session,
            terminate_connection=True,
        )

    schedule_df = get_schedule(
        db_schema=db_schema,
        country_list=country_list,
        date_range=date_range,
        start_date=start_date,
        end_date=end_date,
        league_list=league_list,
        session=session,
    )

    match_ids_schedule = schedule_df["match_id"].unique().tolist()

    match_odds_sql_string = f"select distinct match_id FROM {db_schema}.{db_table}"
    current_match_odds_df = read_sql(match_odds_sql_string, session=session)

    current_match_odds_match_ids = current_match_odds_df["match_id"].unique().tolist()

    match_id_list_remaining = list(
        set(match_ids_schedule).difference(set(current_match_odds_match_ids))
    )

    for match_id in match_id_list_remaining:
        match_odds_df = get_match_odds_df(
            match_id=match_id, db_schema=db_schema, session=session, api_key=api_key
        )

        if not match_odds_df.empty:
            bulk_upsert_write_sql(
                match_odds_df, dbtable=db_table, dbschema=db_schema, session=session
            )

        time.sleep(wait_time)


def get_match_odds_df(
    match_id, db_schema=None, session=None, response_json=None, api_key=None
):
    try:
        if not response_json:
            if not api_key:
                api_key = all_sports_api_key

            api_client = FootApiHarvester(api_key=api_key)
            response_json = api_client.get_match_response_json(
                match_id=match_id, stat_type="odds"
            )

        if not db_schema:
            db_schema = "football_data"

        if not session:
            session = create_session()

        home_team, away_team = get_home_away_team(
            match_id=match_id, db_schema=db_schema, session=session
        )

        dict_mappings = {
            "1": "price1",
            "2": "price2",
            "X": "price3",
            "1X": "price1",
            "X2": "price2",
            "12": "price3",
            "Yes": "price1",
            "No": "price2",
            "Over": "price1",
            "Under": "price2",
            home_team: "price1",
            away_team: "price2",
            "No goal": "price3",
        }

        rename_cols = MATCH_ODDS_DATAFRAME["rename_cols"]

        response_df = pd.json_normalize(response_json["markets"])
        columns = response_df.columns.to_list()

        for col in columns:
            if col != "choices":
                response_df.loc[:, "choices"] = response_df.apply(
                    lambda x: append_to_dict_list(col, x[col], x["choices"]), axis=1
                )

        list_values = response_df["choices"].to_list()
        single_list = list(np.concatenate(list_values).flat)
        unpacked_response_df = pd.json_normalize(single_list)

        unpacked_response_df["fractionalValue"] = unpacked_response_df[
            "fractionalValue"
        ].apply(lambda x: float(x.split("/")[0]) / float(x.split("/")[1]))
        unpacked_response_df["initialFractionalValue"] = unpacked_response_df[
            "initialFractionalValue"
        ].apply(lambda x: float(x.split("/")[0]) / float(x.split("/")[1]))

        unpacked_response_df.rename(columns=rename_cols, inplace=True)

        unpacked_response_df["match_id"] = match_id
        unpacked_response_df["asian_handicap"] = unpacked_response_df.query(
            "market_name == 'Asian handicap' "
        )["name"].apply(lambda x: float(x.split(" ")[0].replace(" ", "")[1:-1]))
        unpacked_response_df["name_new"] = unpacked_response_df.query(
            "market_name == 'Asian handicap' "
        )["name"].apply(lambda x: x.split(") ")[1])
        unpacked_response_df["name_new"] = unpacked_response_df.apply(
            lambda x: final_name_check(x["name"], x["name_new"]), axis=1
        )
        unpacked_response_df["value"] = (
            unpacked_response_df["name_new"].astype(str).map(dict_mappings)
        )
        unpacked_response_df["pricing"] = unpacked_response_df.apply(
            lambda x: {x["value"]: x["fractionalValue"]}, axis=1
        )

        groupby_cols = [
            "match_id",
            "odds_id",
        ]

        groupby_df = (
            unpacked_response_df.groupby(groupby_cols)["pricing"]
            .apply(list)
            .reset_index()
        )
        groupby_df["pricing"] = groupby_df["pricing"].apply(
            lambda x: dict(ChainMap(*x[::-1]))
        )

        temp_df = pd.DataFrame(groupby_df["pricing"].to_list())[
            ["price1", "price2", "price3"]
        ]
        temp_df = pd.concat([groupby_df[["match_id", "odds_id"]], temp_df], axis=1)

        main_df = (
            unpacked_response_df[
                [
                    "match_id",
                    "odds_id",
                    "market_id",
                    "market_name",
                    "point1",
                    "asian_handicap",
                ]
            ]
            .drop_duplicates()
            .reset_index(drop=True)
        )

        odds_df = (
            pd.merge(temp_df, main_df, on=["match_id", "odds_id"])
            .sort_values(["market_name", "point1"])
            .reset_index(drop=True)
        )

        return odds_df

    except Exception as exc:
        print(
            f"{exc}:get_all_sports_api_match_details_dataframe was unable to process, check api_key or source data"
        )
        return pd.DataFrame()


def get_home_away_team(match_id, db_schema=None, session=None):
    if not session:
        session = create_session()

    if not db_schema:
        db_schema = "football_data"

    sql_string = f"select * from {db_schema}.schedule where cast(match_id as char)= '{match_id}' "

    df = read_sql(sql_string, session=session)

    away_team = df["away_team_name"].iloc[0]
    home_team = df["home_team_name"].iloc[0]

    return home_team, away_team


def final_name_check(name, name_new):
    if str(name_new) == "nan":
        return name

    return name_new
