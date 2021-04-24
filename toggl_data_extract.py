from datetime import date, datetime, timedelta
import json
from os import environ
from os.path import join
import sqlite3

import numpy as np
import pandas as pd

from toggl.api_client import TogglClientApi

# api documentaion: https://github.com/toggl/toggl_api_docs/blob/master/reports.md#request-parameters
# data documentation: https://github.com/toggl/toggl_api_docs/blob/master/reports/detailed.md#data-array

with open(join(environ["HOME"], "repos/toggl_api/config.json")) as json_data_file:
    data = json.load(json_data_file)

settings = data["toggl_config"]

client = TogglClientApi(settings)
client.get_projects().json()

def addYears(d, years):
    """Takes date and adds years to it
    
    Args:
        d (datetime): Date to increment years
        years (int): How many years to increment the date
    
    Returns:
        Date plus how many years you want to increment
    """
    try:
    #Return same day of the current year        
        return d.replace(year = d.year + years)
    except ValueError:
        #If not same day, it will return other, i.e.  February 29 to March 1 etc.        
        return d + (date(d.year + years, 1, 1) - date(d.year, 1, 1))

# establish the base year to pull data from
base_year = 2019
# create base_start_date as Jan 1st from base year
base_start_date = date(base_year, 1, 1)
# create base_end_date as Dec 31st from base year
base_end_date = date(base_year, 12, 31)

# create list of yearly date ranges for each year up until current year
# for example: [[datetime.date(2019, 1, 1), datetime.date(2019, 12, 31)]]
date_range_list = []
for i in range(0, datetime.today().year - base_start_date.year + 1):
    date_range_list.append([addYears(base_start_date, i), addYears(base_end_date, i)])

# connect to sqlite database
conn = sqlite3.connect(join(environ["HOME"], "repos/toggl_api/toggl_api.sqlite"))
# query toggl_projects table in sqlite database to grab all project id's
projects_id_df = pd.read_sql("select distinct id from toggl_projects", conn)
# pull list of project id's from toggl_projects table in sqlite database
projects_id_list = projects_id_df.id.tolist()

df_list = []
# loop through each project_id
for project in projects_id_list:
    # loop through each year date range to pull data
    for date_range in date_range_list:
        # api seems to only pull one page at a time - this loops through each page for each project to get results
        for page in range(1, 100):
            # grab data from api
            data = client.get_project_times(str(project), date_range[0], date_range[1], extra_params={"page": page})
            # check if data was pulled
            if len(data["data"]) > 0:
                print("Data found!")
                # add data to dataframe
                df = pd.DataFrame(data["data"])
                # append dataframe to df_list to concat down below
                df_list.append(df)
            # if no data found, then break loop
            else:
                break

# concatenate all the dataframes in the df_list into one dataframe
df_final = pd.concat(df_list)
# data cleaning
df_final["start"] = pd.to_datetime(df_final["start"])
df_final["end"] = pd.to_datetime(df_final["end"])

# convert columns to string
string_convert_list = ["use_stop", "is_billable", "tags"]
for col in string_convert_list:
    df_final[col] = df_final[col].astype(str)

# the "tags" data comes in as a list - convert to just a comma separated string
df_final["tags"] = df_final["tags"].str.replace("[", "").str.replace("]", "").str.replace("'", "")
df_final.loc[df_final.tags == "", "tags"] = np.nan

# create dur_secs column as the number of seconds between the start and end
df_final["dur_secs"] = (df_final["end"] - df_final["start"]).astype("timedelta64[s]")

# write the data to the table in sqlite database
df_final.to_sql("toggl_data", conn, if_exists="replace", index=False)
# pd.read_sql("select * from toggl_data", conn)