from os import environ
from os.path import join
import sqlite3

import pandas as pd

import toggl_extract as te

# connect to ToggleApi class
toggl_client = te.TogglApi()

# pull toggl projects data from api
projects_data_list = toggl_client.get_toggl_projects()

# put toggle projects data into dataframe
projects_df = pd.DataFrame(projects_data_list)

# data cleanup
projects_df["at"] = pd.to_datetime(projects_df["at"])
projects_df["created_at"] = pd.to_datetime(projects_df["created_at"])
convert_to_string = ["billable", "is_private", "active", "template", "auto_estimates"]
for col in convert_to_string:
    projects_df[col] = projects_df[col].astype(str)

# connect to sqlite database
conn = sqlite3.connect(join(environ["HOME"], "repos/toggl_api/toggl_api.sqlite"))

# push toggl projects data to sqlite database
projects_df.to_sql("toggl_projects", conn, if_exists="replace", index=False)
