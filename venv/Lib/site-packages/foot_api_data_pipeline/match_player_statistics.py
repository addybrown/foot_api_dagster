import pandas as pd
import numpy as np
import re
import os
import time
from dotenv import load_dotenv

load_dotenv()
all_sports_api_key = os.getenv("all_sports_api_key")

from services.sql_services import (
    read_sql,
    write_sql,
    check_table_exists,
    create_session,
    execute_sql,
    bulk_upsert_write_sql,
)

from foot_api_harvesting.utils import FootApiHarvester
from foot_api_data_pipeline.variables import (
    PLAYER_TABLE_VARIABLES,
    RELEVANT_LEAGUES,
    PLAYER_MATCH_STATISTICS_DATAFRAME,
)
from foot_api_data_pipeline.pipeline_services import get_schedule


def update_player_match_statistics_table(
    db_table=None,
    db_schema=None,
    session=None,
    wait_time=3,
    api_key=None,
    date_range=3,
    start_date=None,
    end_date=None,
    country_list=None,
    league_list=None,
    **kwargs,
):
    if not session:
        session = create_session()

    if not db_table:
        db_table = "player_match_statistics"

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

    if not check_table_exists(db_table=db_table, db_schema=db_schema, session=session):
        player_match_statistics_df = get_player_match_statitics_df(
            match_id="9576069", player_id="935543", api_key=api_key
        )

        write_sql(
            player_match_statistics_df,
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

    schedule_match_ids = schedule_df["match_id"].astype(str).unique().tolist()

    rating_sql_string = f"""
        SELECT distinct player_rating.match_id,
                        player_rating.player_id
        FROM {db_schema}.{db_table}
        RIGHT JOIN {db_schema}.player_rating
        ON player_match_statistics.match_id = player_rating.match_id
            AND player_match_statistics.player_id = player_rating.player_id
        WHERE player_match_statistics.match_id is null
    """

    rating_df = read_sql(rating_sql_string, session=session)

    rating_df["player_id"] = rating_df["player_id"].astype(str)
    rating_df["match_id"] = rating_df["match_id"].astype(str)

    rating_df = rating_df[rating_df["match_id"].isin(schedule_match_ids)]

    rating_df = (
        rating_df.groupby("match_id")
        .agg({"player_id": lambda x: list(x)})
        .reset_index()
    )

    for row in range(0, len(rating_df)):
        existing_df_sql_string = f"""
            SELECT DISTINCT * FROM {db_schema}.{db_table} LIMIT 1
        """
        existing_df = read_sql(existing_df_sql_string, session=session)

        match_id = rating_df.loc[row, "match_id"]
        player_ids = rating_df.loc[row, "player_id"]

        all_dfs = []
        for player in player_ids:
            df_row = get_player_match_statitics_df(
                match_id=match_id, player_id=player, api_key=api_key
            )
            all_dfs.append(df_row)
            time.sleep(1)

        match_df = pd.concat(all_dfs, axis=0)

        new_df_cols = match_df.columns.tolist()
        cols_to_add = [
            column for column in new_df_cols if column not in existing_df.columns
        ]

        if cols_to_add:
            for column in cols_to_add:
                create_col_sql_string = (
                    f"alter table {db_schema}.{db_table} add {column} int;"
                )
                execute_sql(
                    create_col_sql_string, session=session, terminate_connection=True
                )

        bulk_upsert_write_sql(
            match_df, dbtable=db_table, dbschema=db_schema, session=session
        )


def get_player_match_statitics_df(
    match_id, player_id, player_match_statistics_response_json=None, api_key=None
):
    try:
        if not player_match_statistics_response_json:
            if not api_key:
                api_key = all_sports_api_key

            api_client = FootApiHarvester(api_key=api_key)
            player_match_statistics_response_json = (
                api_client.get_match_player_response_json(
                    match_id, player_id, "statistics"
                )
            )

        rename_cols = PLAYER_MATCH_STATISTICS_DATAFRAME["rename_cols"]
        drop_cols = PLAYER_MATCH_STATISTICS_DATAFRAME["drop_cols"]

        player_match_statistics_df = pd.json_normalize(
            player_match_statistics_response_json
        )
        player_match_statistics_df["match_id"] = match_id

        for column in player_match_statistics_df.columns:
            if "statistics." in column:
                new_col_value = column.split(".")[1]
                player_match_statistics_df.rename(
                    columns={column: new_col_value}, inplace=True
                )

        player_match_statistics_df.rename(columns=rename_cols, inplace=True)
        new_cols = [
            column
            for column in player_match_statistics_df.columns
            if column not in drop_cols
        ]
        player_match_statistics_df = player_match_statistics_df[new_cols]

        player_match_statistics_df.insert(
            0, "player_name", player_match_statistics_df.pop("player_name")
        )
        player_match_statistics_df.insert(
            0, "player_id", player_match_statistics_df.pop("player_id")
        )
        player_match_statistics_df.insert(
            0, "match_id", player_match_statistics_df.pop("match_id")
        )

        for column in player_match_statistics_df.columns:
            new_value = re.sub("(?<!^)(?=[A-Z])", "_", column).lower()
            player_match_statistics_df.rename(columns={column: new_value}, inplace=True)

        return player_match_statistics_df

    except Exception as exc:
        print(
            f"{exc}:get_all_sports_api_player_match_statistics_dataframe was unable to process, check api_key or source data"
        )
        return pd.DataFrame()
