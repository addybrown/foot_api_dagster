SCHEDULE_VARIABLES = {
    "rename_cols": {
        "id": "match_id",
        "tournament_category_id": "country_id",
        "tournament_category_name": "country_name",
        "tournament_id": "tournament_id",
        "tournament_name": "tournament_name",
        "tournament_uniqueTournament_id": "league_id",
        "tournament_uniqueTournament_name": "league_name",
        "awayScore_current": "away_score",
        "homeScore_current": "home_score",
        "awayTeam_id": "away_team_id",
        "homeTeam_id": "home_team_id",
        "awayTeam_name": "away_team_name",
        "homeTeam_name": "home_team_name",
        "startTimestamp": "match_start_time",
        "status_type": "status",
    },
    "main_cols": [
        "match_id",
        "match_start_time",
        "away_score",
        "home_score",
        "away_team_id",
        "away_team_name",
        "home_team_id",
        "home_team_name",
        "league_id",
        "league_name",
        "tournament_id",
        "tournament_name",
        "country_id",
        "country_name",
        "status",
    ],
}

MATCH_SHOTMAP_DATAFRAME = {
    "main_cols": [
        "match_id",
        "shot_id",
        "player_name",
        "player_id",
        "bodyPart",
        "time",
        "timeSeconds",
        "situation",
        "player_position",
        "playerCoordinates_x",
        "playerCoordinates_y",
        "blockCoordinates_x",
        "blockCoordinates_y",
        "shotType",
        "xg",
        "xgot",
    ]
}

MATCH_INCIDENT_DATAFRAME = {
    "rename_cols": {
        "match_id": "match_id",
        "awayScore": "away_score",
        "homeScore": "home_score",
        "incidentType": "incident_type",
        "text": "time_status",
        "time": "match_clock",
        "incidentClass": "incident_class",
        "player.name": "player_name",
        "player.id": "player_id",
        "player.position": "player_position",
        "playerIn.id": "player_id_in",
        "playerIn.name": "player_name_in",
        "playerOut.id": "player_id_out",
        "playerOut.name": "player_name_out",
        "injury": "injury",
    },
    "main_cols": [
        "match_id",
        "player_id",
        "player_name",
        "player_position",
        "incident_type",
        "incident_class",
        "home_score",
        "away_score",
        "match_clock",
        "time_status",
        "player_id_in",
        "player_name_in",
        "player_id_out",
        "player_name_out",
        "injury",
    ],
}


PLAYER_TEAM_DATAFRAME = {
    "rename_cols": {
        "player_preferredFoot": "preferred_foot",
        "player_proposedMarketValue": "market_value",
        "player_proposedMarketValueRaw_currency": "market_value_currency",
        "player_dateOfBirthTimestamp": "date_of_birth",
        "player_jerseyNumber": "jersey_number",
        "player_team_id": "team_id",
        "player_team_name": "team_name",
        "player_team_national": "team_national",
        "player_team_primaryUniqueTournament_id": "league_id",
        "player_team_primaryUniqueTournament_name": "league_name",
        "player_team_country_name": "team_country_name",
        "player_country_name": "player_nationality",
    },
    "main_cols": [
        "player_id",
        "player_name",
        "player_position",
        "date_of_birth",
        "jersey_number",
        "preferred_foot",
        "player_height",
        "market_value",
        "market_value_currency",
        "team_id",
        "team_name",
        "team_national",
        "league_id",
        "league_name",
        "team_country_name",
        "player_nationality",
    ],
}

MATCH_LINEUP_DATAFRAME = {
    "main_cols": [
        "match_id",
        "away_formation",
        "away_players",
        "home_formation",
        "home_players",
    ]
}

SEASONS_DATAFRAME = {
    "rename_cols": {"id": "season_id", "name": "season_name"},
    "main_cols": [
        "season_id",
        "season_name",
        "year",
        "start_year",
        "end_year",
        "league_id",
    ],
}


DAILY_UPLOAD_FUNCTIONS_TABLES = {
    "player_table": {
        "main_table": {"schema": "football_data", "table": "players"},
        "england": {"schema": "football_data", "table": "england_players"},
    },
    "team_table": {
        "main_table": {"schema": "football_data", "table": "teams"},
        "england": {"schema": "football_data", "table": "england_teams"},
    },
    "schedule_table": {
        "main_table": {"schema": "football_data", "table": "schedule"},
        "england": {"schema": "football_data", "table": "england_schedule"},
    },
    "shot_table": {
        "main_table": {"schema": "football_data", "table": "shot_details"},
        "england": {"schema": "football_data", "table": "england_shot_details"},
    },
}

PLAYER_MATCH_STATISTICS_DATAFRAME = {
    "rename_cols": {"player.id": "player_id", "player.name": "player_name"},
    "drop_cols": [
        "player.dateOfBirthTimestamp",
        "player.firstName",
        "player.marketValueCurrency",
        "player.userCount",
        "team.gender",
        "team.id",
        "team.name",
        "team.nameCode",
        "team.national",
        "team.shortName",
        "team.slug",
        "team.sport.id",
        "team.sport.name",
        "team.sport.slug",
        "team.teamColors.primary",
        "team.teamColors.secondary",
        "team.teamColors.text",
        "team.type",
        "team.userCount",
        "team.disabled" "player.slug",
        "player.lastName",
        "player.position",
        "player.shortName",
        "player.slug",
        "team.disabled",
    ],
}


MATCH_DETAILS_DATAFRAME = {
    "rename_cols": {
        "venue_rename_cols": {
            "stadium.name": "stadium_name",
            "stadium.capacity": "stadium_capacity",
            "city.name": "venue_city_name",
            "country.name": "venue_country_name",
        },
        "referee_rename_cols": {
            "id": "referee_id",
            "name": "referee_name",
            "country.name": "referee_country_name",
        },
    }
}

MATCH_ODDS_DATAFRAME = {
    "rename_cols": {
        "id": "odds_id",
        "sourceId": "source_id",
        "marketId": "market_id",
        "marketName": "market_name",
        "structureType": "structure_type",
        "choiceGroup": "point1",
    }
}

DAILY_UPLOAD_FUNCTIONS_LOG_PATHS = {
    "player_table": r"C:\Users\adams\Documents\personal_projects\coding_content\content\football\stats_processing\all_sports_api_data_processing\daily_upload_logs"
}


SEASONS = {
    "80": "1980",
    "81": "1981",
    "82": "1982",
    "83": "1983",
    "84": "1984",
    "85": "1985",
    "86": "1986",
    "87": "1987",
    "88": "1988",
    "89": "1989",
    "90": "1990",
    "91": "1991",
    "92": "1992",
    "93": "1993",
    "94": "1994",
    "95": "1995",
    "96": "1996",
    "97": "1997",
    "98": "1998",
    "99": "1999",
    "00": "2000",
    "01": "2001",
    "02": "2002",
    "03": "2003",
    "04": "2004",
    "05": "2005",
    "06": "2006",
    "07": "2007",
    "08": "2008",
    "09": "2009",
    "10": "2010",
    "11": "2011",
    "12": "2012",
    "13": "2013",
    "14": "2014",
    "15": "2015",
    "16": "2016",
    "17": "2017",
    "18": "2018",
    "19": "2019",
    "20": "2020",
    "21": "2021",
    "22": "2022",
    "23": "2023",
    "24": "2024",
    "25": "2025",
}

PLAYER_TABLE_VARIABLES = {
    "player_table_country_names": [
        "England",
        # "Spain",
        # "Portugal",
        # "Germany",
        # "France",
        "Europe",
    ]
}

RELEVANT_LEAGUES = [
    "Premier League",
    # "Bundesliga",
    # "Ligue 1",
    # "Ligue 2",
    # "LaLiga",
    # "Championship",
    # "League One",
    # "League Two",
    # "2. Bundesliga",
    # "MLS",
]

PLAYER_STATISTICS = {
    "shared_cols": [
        "match_id",
        "player_id",
        "team_id",
        "team_name",
        "player_name",
        "player_position",
        "position",
        "home_away",
    ],
    "player_rating": {
        "main_cols": [
            "minutes_played",
            "player_rating",
        ]
    },
    "player_passing": {
        "main_cols": [
            "accurate_crosses",
            "accurate_long_balls",
            "accurate_passes",
            "key_passes",
            "total_passes",
            "total_long_balls",
        ]
    },
    "player_shooting": {
        "main_cols": [
            "shots_off_target",
            "on_target_scoring_attempt",
            "big_chances_created",
            "big_chances_missed",
        ]
    },
    "player_defense": {
        "main_cols": [
            "aerial_won",
            "aerial_lost",
            "blocked_scoring_attempt",
            "total_tackle",
            "clearance_offline",
            "duel_won",
            "duel_lost",
            "interception_won",
            "total_clearance",
            "last_man_tackle",
            "won_contents",
            "challenges_lost",
        ]
    },
    "player_mistake": {
        "main_cols": [
            "dispossessed",
            "own_goals",
            "possession_lost_ctrl",
            "error_lead_to_goal",
            "error_lead_to_shot",
            "penalty_conceded",
        ]
    },
    "player_misc": {
        "main_cols": [
            "fouls",
            "total_offsides",
            "touches",
            "was_fouled",
            "goal_assists",
            "penalty_misses",
            "pk_missed",
            "pk_goal",
        ]
    },
    "rename_cols": {
        "statistics.minutesPlayed": "minutes_played",
        "statistics.rating": "player_rating",
        "statistics.accurateCross": "accurate_crosses",
        "statistics.accurateLongBalls": "accurate_long_balls",
        "statistics.accuratePass": "accurate_passes",
        "statistics.keyPass": "key_passes",
        "statistics.totalPass": "total_passes",
        "statistics.totalLongBalls": "total_long_balls",
        "statistics.shotOffTarget": "shots_off_target",
        "statistics.onTargetScoringAttempt": "on_target_scoring_attempt",
        "statistics.bigChanceCreated": "big_chances_created",
        "statistics.bigChanceMissed": "big_chances_missed",
        "statistics.hitWoodwork": "hit_woodwork",
        "statistics.aerialWon": "aerial_won",
        "statistics.aerialLost": "aerial_lost",
        "statistics.blockedScoringAttempt": "blocked_scoring_attempt",
        "statistics.totalTackle": "total_tackle",
        "statistics.clearanceOffLine": "clearance_offline",
        "statistics.duelWon": "duel_won",
        "statistics.duelLost": "duel_lost",
        "statistics.interceptionWon": "interception_won",
        "statistics.totalClearance": "total_clearance",
        "statistics.lastManTackle": "last_man_tackle",
        "statistics.wonContest": "won_contents",
        "statistics.challengeLost": "challenges_lost",
        "statistics.dispossessed": "dispossessed",
        "statistics.ownGoals": "own_goals",
        "statistics.possessionLostCtrl": "possession_lost_ctrl",
        "statistics.errorLeadToAGoal": "error_lead_to_goal",
        "statistics.errorLeadToAShot": "error_lead_to_shot",
        "statistics.penaltyConceded": "penalty_conceded",
        "statistics.fouls": "fouls",
        "statistics.totalOffside": "total_offsides",
        "statistics.touches": "touches",
        "statistics.wasFouled": "was_fouled",
        "statistics.goalAssist": "goal_assists",
        "statistics.penaltyMiss": "penalty_misses",
        "statistics.penaltyShootoutMiss": "pk_missed",
        "statistics.penaltyShootoutGoal": "pk_goal",
        "statistics.totalKeeperSweeper": "total_keeper_sweeper",
    },
}
