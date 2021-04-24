from datetime import date, datetime, timedelta
from os import environ
from os.path import join
import sqlite3

import numpy as np
import pandas as pd

import toggl_extract as te

toggl_client = te.TogglApi()

# helper functions
def add_years(dt, years):
    """Takes date and adds years to it
    
    Args:
        dt (datetime): Date to increment years
        years (int): How many years to increment the date
    
    Returns:
        Date plus how many years you want to increment
    """
    try:
    #Return same day of the current year        
        return dt.replace(year=dt.year + years)
    except ValueError:
        #If not same day, it will return other, i.e.  February 29 to March 1 etc.        
        return dt + (date(dt.year + years, 1, 1) - date(dt.year, 1, 1))

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
    date_range_list.append([add_years(base_start_date, i), add_years(base_end_date, i)])

# connect to sqlite database
conn = sqlite3.connect(join(environ["HOME"], "repos/toggl_api/toggl_api.sqlite"))
# query toggl_projects table in sqlite database to grab all project id's
projects_id_df = pd.read_sql("SELECT DISTINCT ID FROM TOGGL_PROJECTS;", conn)
# pull list of project id's from toggl_projects table in sqlite database
projects_id_list = projects_id_df.id.tolist()

df_list = []
# loop through each project_id
for project in projects_id_list:
    # loop through each year date range to pull data
    for date_range in date_range_list:
        toggl_api_data = toggl_client.get_toggl_log_data(project_id=project, date_range_list=date_range)
        # iterate through the toggl_api_data to get data into a dataframe
        for row in toggl_api_data:
            df = pd.DataFrame(row)
            df_list.append(df)

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

print("Writing data to sqlite table")
# write the data to the table in sqlite database
df_final.to_sql("toggl_data", conn, if_exists="replace", index=False)
