from datetime import date, datetime, timedelta
import json
from os import environ
from os.path import join
import sqlite3

import pandas as pd

from toggl.api_client import TogglClientApi

with open(join(environ["HOME"], "repos/toggl_api/config.json")) as json_data_file:
    data = json.load(json_data_file)

settings = data["toggl_config"]

client = TogglClientApi(settings)

projects_data = client.get_projects().json()

projects_df = pd.DataFrame(projects_data)

# data cleanup
projects_df["at"] = pd.to_datetime(projects_df["at"])
projects_df["created_at"] = pd.to_datetime(projects_df["created_at"])
convert_to_string = ["billable", "is_private", "active", "template", "auto_estimates"]
for col in convert_to_string:
    projects_df[col] = projects_df[col].astype(str)

conn = sqlite3.connect(join(environ["HOME"], "repos/toggl_api/togg_api.sqlite"))

projects_df.to_sql("toggl_projects", conn, if_exists="replace", index=False)
