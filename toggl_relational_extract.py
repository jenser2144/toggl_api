from datetime import date, datetime, timedelta
import json
from os import environ
from os.path import join

import numpy as np
import pandas as pd
import psycopg2
import pytz
from sqlalchemy import create_engine

import toggl_extract as te

# Open config file
with open(join(environ["HOME"], "repos/toggl_api/config.json")) as json_data_file:
    data = json.load(json_data_file)

# Grab the database_config from the json data
db_settings = data["database_config"]

# Define user and database from config
user = db_settings["user"]
db = db_settings["database"]
connection = db_settings["connection"]

# Create engine for postgres connection
engine = create_engine(f"postgresql://{user}@{connection}/{db}")


### Start of helper functions
def duplicate_entry_check(table_name, column_name, engine, dataframe_name, table_columns):
    """Pull data from table and check against dataframe if the records in the dataframe already exist in the table.
    If so, filter those out of the dataframe.
    
    Args:
        table_name (str): Name of the table
        column_name (str): Name of the column in the table to pull the records from the table
        engine (SQLAlchemy engine object): SQLAlchemy engine object
        dataframe_name (Pandas DataFrame object): Pandas DataFrame object containing the data
        table_columns (list): List of columns for the final table to re-order the columns in the dataframe
    
    Returns:
        merged_df (Pandas DataFrame object): Filtered dataframe of non-duplicate records in table
    
    """
    # Split column name on comma - create list
    column_name_list = column_name.split(",")
    # Strip out whitespace from each value in the list
    column_name_list = [column.strip() for column in column_name_list]

    # Query the table table
    table_df = pd.read_sql(f"SELECT ID AS TABLE_ID, {column_name} FROM {table_name};", engine)

    # Merge the table data to the dataframe
    merged_df = dataframe_name.merge(right=table_df, on=column_name_list, how="left")
    
    # Filter out duplicate rows
    merged_df = merged_df[merged_df.table_id.isnull()]
    
    # Delete the table_id column
    del merged_df["table_id"]
    
    # Re-order columns
    merged_df = merged_df[table_columns]
    
    return merged_df


def foreign_key_grab(table_name, column_name, engine, dataframe_name, foreign_key_name):
    """Grab the foreign key value from the reference table
    
    Args:
        table_name (str): Name of the table
        column_name (str): Name of the column in the table to pull the records from the table
        engine (SQLAlchemy engine object): SQLAlchemy engine object
        dataframe_name (Pandas DataFrame object): Pandas DataFrame object containing the data
        foreign_key_name (str): Name of the column in the table which is the foreign key
    
    Returns:
        merge_df (Pandas DataFrame object): Pandas DataFrame object containing the foreign key
    """
    
    # Query the table
    table_data_df = pd.read_sql(f"SELECT ID AS TABLE_ID, {column_name} FROM {table_name};", engine)
    
    # Merge the table data to the dataframe
    merged_df = dataframe_name.merge(right=table_data_df, on=column_name, how="left")
    
    # Rename column
    merged_df.rename(columns={"table_id": foreign_key_name}, inplace=True)
    
    return merged_df


def convert_to_utc(datetime_obj):
    """Convert datetime object to UTC. This datetime object must already have timezone specified.
    
    Args:
        datetime_obj (datetime object): Date of the entry
    
    Returns:
        utc_datetime (datetime object): Date coverted to UTC
    
    """
    
    # Convert to UTC
    utc_datetime = datetime_obj.astimezone(pytz.utc).replace(tzinfo=None)
    
    return utc_datetime


def add_data_to_table(dataframe_name, engine, table_name):
    """Append data to table if any exists in dataframe
    
    Args:
        dataframe_name (Pandas DataFrame object): Pandas DataFrame containing data to write to table
        engine (SQLAlchemy engine object)
        table_name (str): Name of the table
    
    Returns:
    
    """
    if not dataframe_name.empty:
        print(f"Adding data to {table_name} table")
        # Append data to postgres table for toggl_projects
        dataframe_name.to_sql(table_name, engine, if_exists="append", index=False)
    else:
        print(f"No new data to add to {table_name}")


def add_years(dt, years):
    """Takes date and adds years to it
    
    Args:
        dt (datetime): Date to increment years
        years (int): How many years to increment the date
    
    Returns:
        Date plus how many years you want to increment
    """
    try:
    # Return same day of the current year        
        return dt.replace(year=dt.year + years)
    except ValueError:
        # If not same day, it will return other, i.e.  February 29 to March 1 etc.        
        return dt + (date(dt.year + years, 1, 1) - date(dt.year, 1, 1))

### End of helper functions

print("Pulling Toggl Projects data")
### Start of pulling toggl projects data
# Connect to ToggleApi class
toggl_client = te.TogglApi()

# Pull toggl projects data from api
projects_data_list = toggl_client.get_toggl_projects()

# Put toggle projects data into dataframe
projects_df_raw = pd.DataFrame(projects_data_list)

### Start of toggl projects data cleanup
# Convert columns to dates
projects_df_raw["at"] = pd.to_datetime(projects_df_raw["at"])
projects_df_raw["created_at"] = pd.to_datetime(projects_df_raw["created_at"])
# List of columns to convert to string
convert_to_string = ["billable", "is_private", "active", "template", "auto_estimates"]
# Convert columns to string
for col in convert_to_string:
    projects_df_raw[col] = projects_df_raw[col].astype(str)
### End of data cleanup

### Start of data transformation for database
# Grab only certain columns
projects_df = projects_df_raw.copy()
projects_df = projects_df[["id", "name", "active", "created_at"]]

# Rename columns
projects_df.rename(columns={"name":"project_name", "created_at":"created_at_date", "active":"active_raw"},
                   inplace=True)

# Create active column as 0 then add 1 for anything that is True
projects_df["active"] = 0
projects_df.loc[projects_df.active_raw == "True", "active"] = 1
# Convert active column to boolean datatype
projects_df["active"] = projects_df["active"].astype("bool")
# Delete active_raw column
del projects_df["active_raw"]
### End of database data transformation

# Checking table and filtering out duplicates values
projects_df = duplicate_entry_check(table_name="toggl_project", column_name="project_name", engine=engine,
    dataframe_name=projects_df, table_columns=["id", "project_name", "created_at_date", "active"])

# Write data to table
add_data_to_table(dataframe_name=projects_df, engine=engine, table_name="toggl_project")

print("Pulling Toggl Entry data")
### Start of pulling Toggl entry data
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

# query toggl_projects table in sqlite database to grab all project id's
projects_id_df = pd.read_sql("SELECT DISTINCT ID FROM TOGGL_PROJECT;", engine)

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
toggl_data_raw_df = pd.concat(df_list).reset_index(drop=True)

# data cleaning
toggl_data_raw_df["start"] = pd.to_datetime(toggl_data_raw_df["start"])
toggl_data_raw_df["end"] = pd.to_datetime(toggl_data_raw_df["end"])
toggl_data_raw_df["updated"] = pd.to_datetime(toggl_data_raw_df["updated"])

# convert columns to string
string_convert_list = ["use_stop", "is_billable", "tags"]
for col in string_convert_list:
    toggl_data_raw_df[col] = toggl_data_raw_df[col].astype(str)

# the "tags" data comes in as a list - convert to just a comma separated string
toggl_data_raw_df["tags"] = toggl_data_raw_df["tags"].str.replace("[", "").str.replace("]", "").str.replace("'", "")
toggl_data_raw_df.loc[toggl_data_raw_df.tags == "", "tags"] = np.nan

# create dur_secs column as the number of seconds between the start and end
toggl_data_raw_df["dur_secs"] = (toggl_data_raw_df["end"] - toggl_data_raw_df["start"]).astype("timedelta64[s]")

print("Transforming Toggl User data")
### Start of Toggl Users data
# Create copy of toggl_data_raw_df to create task data
user_data_df = toggl_data_raw_df.copy()

# Get distinct values of the uid and user columns
user_data_df = user_data_df[["uid", "user"]].drop_duplicates()

# Rename columns
user_data_df.rename(columns={"uid": "id", "user": "name"}, inplace=True)

# Checking table and filtering out duplicates values
user_data_df = duplicate_entry_check(table_name="toggl_user", column_name="name", engine=engine,
    dataframe_name=user_data_df, table_columns=["id", "name"])

# Write data to table
add_data_to_table(dataframe_name=user_data_df, engine=engine, table_name="toggl_user")
### End of Toggl Users data

print("Transforming Toggl Tasks data")
### Start of Toggl Tasks data
# Create copy of toggl_data_raw_df to create task data
task_df = toggl_data_raw_df.copy()

# Get distinct values of the description column
task_df = task_df[["description"]].drop_duplicates()

# Rename columns
task_df.rename(columns={"description": "task_name"}, inplace=True)

# Iterate through each row to create an id row_number
id_list = []
for i, task in enumerate(task_df.task_name.tolist()):
    id_list.append(i + 1)

# Add the id column to the dataframe from the id_list
task_df["id"] = id_list

# Checking table and filtering out duplicates values
task_df = duplicate_entry_check(table_name="toggl_task", column_name="task_name", engine=engine,
    dataframe_name=task_df, table_columns=["id", "task_name"])

# Write data to table
add_data_to_table(dataframe_name=task_df, engine=engine, table_name="toggl_task")
### End of Toggl Tasks data

print("Transforming Toggl Tag data")
### Start of Toggl Tag data
# Create copy of toggl_data_raw_df to create tag data
tag_df = toggl_data_raw_df.copy()

# Get distinct values of the tags column
tag_df = tag_df[tag_df.tags.notnull()][["tags"]].drop_duplicates().reset_index(drop=True)

# Split out the tags column into multiple columns - use comma as delimiter
tag_df = tag_df.tags.str.split(pat=",", expand=True)

# Create new column names for dataframe
tag_column_names = ["tag_" + str(col + 1) for col in tag_df.columns]

# Rename columns in dataframe
tag_df.columns = tag_column_names

# Add the data to a list
tag_row_data = tag_df.values.tolist()

# Add data from list to set to get unique values
tag_set = set()
# Iterate through tag_row_data
for tag_row in tag_row_data:
    # Iterate through the tag_row
    for val in tag_row:
        # If there is data in the row
        if val:
            # Strip whitespace
            val = val.strip()
            # Add val to the tag_set
            tag_set.add(val)

# Create list of id's based on how many items are in the set
id_list = [i + 1 for i in range(len(list(tag_set)))]
# Convert the set to a list
tag_list = list(tag_set)
# Zip the id_list and tag_list into a nested list
tag_full_list = list(zip(id_list, tag_list))
# Create dataframe from the tag_full_list
tag_unique_df = pd.DataFrame(data=tag_full_list, columns=["id", "tag_name"])

# Checking table and filtering out duplicates values
tag_unique_df = duplicate_entry_check(table_name="toggl_tag", column_name="tag_name", engine=engine,
    dataframe_name=tag_unique_df, table_columns=["id", "tag_name"])

# Write data to table
add_data_to_table(dataframe_name=tag_unique_df, engine=engine, table_name="toggl_tag")
### End of Toggl Tag data

print("Transforming Toggl Entry data")
### Start of Toggl Entry data
# Create copy of toggl_data_raw_df to create entry data
entry_data_df = toggl_data_raw_df.copy()

# Get distinct values of the uid and user columns
entry_data_df = entry_data_df[["id", "pid", "uid", "description", "start", "end", "updated"]].drop_duplicates()

# Rename columns
entry_data_df.rename(columns={"uid": "toggl_user_id", "pid": "toggl_project_id", "description": "task_name",
                              "start": "start_date", "end": "end_date", "updated": "update_date"},
                     inplace=True)

# Get the id from the toggl_task table for the foreign key
entry_data_df = foreign_key_grab(table_name="toggl_task", column_name="task_name", engine=engine, 
    dataframe_name=entry_data_df, foreign_key_name="toggl_task_id")

# Create list of columns for the entry data
entry_data_columns = ["id", "toggl_project_id", "toggl_task_id", "toggl_user_id", "start_date", "end_date",
                      "update_date"]

# Checking table and filtering out duplicates values
entry_data_df = duplicate_entry_check(table_name="toggl_entry", column_name="id", engine=engine,
    dataframe_name=entry_data_df, table_columns=entry_data_columns)

# Create empty lists for each date column
start_date_utc_list = []
end_date_utc_list = []
update_date_utc_list = []
# Iterate through each row in the dataframe and convert date columns to utc
for row in entry_data_df.values.tolist():
    # Convert start_date to utc
    start_date_utc_list.append(convert_to_utc(row[4]))
    # Convert end_date to utc
    end_date_utc_list.append(convert_to_utc(row[5]))
    # Convert update_date to utc
    update_date_utc_list.append(convert_to_utc(row[6]))

# Adding the UTC converted dates to the dataframe columns
entry_data_df["start_date"] = start_date_utc_list
entry_data_df["end_date"] = end_date_utc_list
entry_data_df["update_date"] = update_date_utc_list

# Write data to table
add_data_to_table(dataframe_name=entry_data_df, engine=engine, table_name="toggl_entry")
### End of Toggl Entry data

print("Transforming Toggl Entry Tag data")
### Start of Toggl Entry Tag data
# Create copy of toggl_data_raw_df to create entry tag data
entry_tag_data_df = toggl_data_raw_df.copy()

# Split out the tags column into multiple columns - use comma as delimiter
entry_tag_data_df[tag_column_names] = entry_tag_data_df.tags.str.split(pat=",", expand=True)

# Create tall table - row per tag
entry_tag_data_tall_df = pd.melt(frame=entry_tag_data_df, id_vars=["id"], value_vars=tag_column_names)

# # Create filter indicator for all rows that 
# entry_tag_data_tall_df.loc[(entry_tag_data_tall_df.variable != "tag_1") &
#                            (entry_tag_data_tall_df.value.isnull()), "filter_ind"] = 1
# entry_tag_data_tall_df = entry_tag_data_tall_df[entry_tag_data_tall_df.filter_ind.isnull()]

# Filter out all rows where the value is null
entry_tag_data_tall_df = entry_tag_data_tall_df[entry_tag_data_tall_df.value.notnull()].reset_index(drop=True)
# Rename value to tag_name to match toggl_tag table
entry_tag_data_tall_df.rename(columns={"value":"tag_name", "id":"toggl_entry_id"}, inplace=True)
# Strip whitespace from the tag_name column
entry_tag_data_tall_df["tag_name"] = entry_tag_data_tall_df.tag_name.str.strip()

# Get the id from the toggl_task table for the foreign key
entry_tag_data_tall_df = foreign_key_grab(table_name="toggl_tag", column_name="tag_name", engine=engine, 
    dataframe_name=entry_tag_data_tall_df, foreign_key_name="toggl_tag_id")

# Delete unneeded columns
del entry_tag_data_tall_df["variable"]
del entry_tag_data_tall_df["tag_name"]

# Checking table and filtering out duplicates values
entry_tag_data_tall_df = duplicate_entry_check(table_name="toggl_entry_tag",
    column_name="toggl_entry_id, toggl_tag_id", engine=engine, dataframe_name=entry_tag_data_tall_df,
    table_columns=["toggl_entry_id", "toggl_tag_id"])

# Write data to table
add_data_to_table(dataframe_name=entry_tag_data_tall_df, engine=engine, table_name="toggl_entry_tag")
### End of Toggl Entry Tag data