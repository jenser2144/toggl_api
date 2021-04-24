from datetime import date, datetime, timedelta
import json
from os import environ
from os.path import join
import sqlite3

import numpy as np
import pandas as pd

from toggl.api_client import TogglClientApi

class TogglApi():
    def __init__(self, config_file=None):
        """Instantiate TogglApi class and connect to toggl api
    
        Args:
            config_file (str, optional): Full path to config json file. Config json file should look like:
                {
                    "toggl_config":{
                        "token": "some_token_string",
                        "user_agent": "toggl_login",
                        "workspace_id": "workspace_id"
                    }
                }
    
        Returns:

        """
        self.config_file = config_file
        if not self.config_file:
            self.config_file = join(environ["HOME"], "repos/toggl_api/config.json")
        with open(self.config_file) as json_data_file:
            data = json.load(json_data_file)

        self.settings = data["toggl_config"]

        self.client = TogglClientApi(self.settings)

    def get_toggl_projects(self):
        """Get projects data from toggl api

        Args:

        Returns:
            projects_data (list): List of dictionaries containing projects data
        
        """
        projects_data = self.client.get_projects().json()
        return projects_data

    def get_toggl_log_data(self, project_id, date_range_list):
        """Pull toggl data from api
        
        Args:
            project_id (int): List of project id's to pull data from
            date_range_list (list): List of date range, each item in list is a datetime object.
                For example: [datetime.date(2019, 1, 1), datetime.date(2019, 12, 31)]
        
        Returns:
            data_list (list): Nested list containing toggl data

        """         
        data_list = []
        # api seems to only pull one page at a time - this loops through each page for each project to get results
        for page in range(1, 100):
            # grab data from api
            data = self.client.get_project_times(str(project_id), date_range_list[0], date_range_list[1], extra_params={"page": page})
            # check if data was pulled
            if len(data["data"]) > 0:
                print("Data found!")
                # append dataframe to df_list to concat down below
                data_list.append(data["data"])
            # if no data found, then break loop
            else:
                break
        return data_list
