import datetime

import pandas as pd
from jira import JIRA

from get_enviromental_variables import get_environmental_variable
from get_issues import get_issues

# Set starting running date
starting_timestamp = (datetime.datetime.now())
# print('STARTING DATETIME: ', starting_timestamp, '\n')

# Access BridgerPay Jira credentials and server url
my_server_url = get_environmental_variable("SERVER_URL")
my_user_name = get_environmental_variable("JIRA_USERNAME")
my_token = get_environmental_variable("JIRA_TOKEN")

# Use credentials and server url to connect code with Jira API
my_jira = JIRA(options={'server': my_server_url}, basic_auth=(my_user_name, my_token))

# Get all projects
projects = my_jira.projects()

# Sort available project keys and names
proj_keys = sorted(project.key for project in projects)
proj_names = sorted(project.name for project in projects)

# Get historical issues data as dataframe containing all information needed from a Jira project
# get_historical = True
get_historical = False
if get_historical:
    print('Get Historical Issues: \n')
    historical_jira_issues = get_issues(my_jira, 'historical')
    print('Historical Issues Dataset Shape:', historical_jira_issues.shape, '\n')
else:
    historical_jira_issues = pd.DataFrame([])

# Get issues data saved as .csv file containing all information needed from a Jira project
print('Get Recent Issues: \n')
jira_issues = get_issues(my_jira, 'last_100')
print('Recent Issues Dataset Shape:', jira_issues.shape, '\n')

# Merge historical and last_X issues from Jira project in a single dataframe
all_jira_issues = pd.concat([historical_jira_issues, jira_issues])
all_jira_issues = all_jira_issues.drop_duplicates()
print('All Project Issues Dataset Shape:', all_jira_issues.shape, '\n')

# Set ending running date
ending_timestamp = datetime.datetime.now()
# print('ENDING DATETIME: ', ending_timestamp, '\n')

# Calculate time needed to run and collect data
total_time_needed = ending_timestamp - starting_timestamp
print('TIME NEEDED: ', total_time_needed)
