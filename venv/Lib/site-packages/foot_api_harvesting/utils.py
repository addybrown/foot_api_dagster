import pandas as pd
import numpy as np
import requests
import os

all_sports_api_key = os.getenv("all_sports_api_key")


class FootApiHarvester:
    def __init__(self, base_url=None, api_key=None):
        self.base_url = base_url if base_url else "https://footapi7.p.rapidapi.com/api"
        self.api_key = api_key if api_key else all_sports_api_key

    def get_response_json_from_endpoint(self, endpoint):
        url = f"{self.base_url}/{endpoint}"
        headers = {
            "X-RapidAPI-Key": self.api_key,
            "X-RapidAPI-Host": "footapi7.p.rapidapi.com",
        }
        response = requests.request("GET", url, headers=headers)
        return response.json()

    def get_schedule_response_json(self, day, month, year):
        url = f"{self.base_url}/matches/{day}/{month}/{year}"
        headers = {
            "X-RapidAPI-Key": self.api_key,
            "X-RapidAPI-Host": "footapi7.p.rapidapi.com",
        }
        response = requests.request("GET", url, headers=headers)
        return response.json()

    def get_match_response_json(self, match_id, stat_type):
        url = f"{self.base_url}/match/{match_id}/{stat_type}"
        headers = {
            "X-RapidAPI-Key": self.api_key,
            "X-RapidAPI-Host": "footapi7.p.rapidapi.com",
        }
        response = requests.request("GET", url, headers=headers)
        return response.json()

    def get_match_team_response_json(self, match_id, team_id, stat_type):
        url = f"{self.base_url}/match/{match_id}/team/{team_id}/{stat_type}"
        headers = {
            "X-RapidAPI-Key": self.api_key,
            "X-RapidAPI-Host": "footapi7.p.rapidapi.com",
        }
        response = requests.request("GET", url, headers=headers)
        return response.json()

    def get_match_player_response_json(self, match_id, player_id, stat_type):
        url = f"{self.base_url}/match/{match_id}/player/{player_id}/{stat_type}"
        headers = {
            "X-RapidAPI-Key": self.api_key,
            "X-RapidAPI-Host": "footapi7.p.rapidapi.com",
        }
        response = requests.request("GET", url, headers=headers)
        return response.json()

    def get_player_response_json(self, player_id, stat_type):
        url = f"{self.base_url}/player/{player_id}/{stat_type}"
        headers = {
            "X-RapidAPI-Key": self.api_key,
            "X-RapidAPI-Host": "footapi7.p.rapidapi.com",
        }
        response = requests.request("GET", url, headers=headers)
        return response.json()

    def get_team_response_json(self, team_id, stat_type):
        url = f"{self.base_url}/team/{team_id}/{stat_type}"
        headers = {
            "X-RapidAPI-Key": self.api_key,
            "X-RapidAPI-Host": "footapi7.p.rapidapi.com",
        }
        response = requests.request("GET", url, headers=headers)
        return response.json()

    def get_league_response_json(self, league_id, endpoint):
        url = f"{self.base_url}/tournament/{league_id}/{endpoint}"
        headers = {
            "X-RapidAPI-Key": self.api_key,
            "X-RapidAPI-Host": "footapi7.p.rapidapi.com",
        }
        response = requests.request("GET", url, headers=headers)
        return response.json()
