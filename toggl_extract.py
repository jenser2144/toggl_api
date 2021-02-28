from datetime import date, datetime, timedelta
import json
from os import environ
from os.path import join
import sqlite3

import pandas as pd

from toggl.api_client import TogglClientApi

class TogglApi():
    def __init__(self, config_file):
        """Instantiate TogglApi class and connect to toggl api
    
        Args:
            config_file (str): Full path to config.json file
    
        Returns:

        """
        with open(join(environ["HOME"], "repos/toggl_api/config.json")) as json_data_file:
            data = json.load(json_data_file)

        settings = data["toggl_config"]

        self.client = TogglClientApi(settings)

    def get_toggl_projects(self):
        """Get projects data from toggl api

        Args:

        Returns:
            dataframe object: Pandas dataframe containing toggl projects data
        
        """
        projects_data = self.client.get_projects().json()
        projects_df = pd.DataFrame(projects_data)

        # data cleanup
        projects_df["at"] = pd.to_datetime(projects_df["at"])
        projects_df["created_at"] = pd.to_datetime(projects_df["created_at"])
        convert_to_string = ["billable", "is_private", "active", "template", "auto_estimates"]
        for col in convert_to_string:
            projects_df[col] = projects_df[col].astype(str)
        
        return projects_df