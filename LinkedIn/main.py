import pandas as pd
from datetime import datetime

from main_aux_run import main_aux_run
from linkedin_concatenate_files import run_concatenate

from get_linkedin_variables import get_linkedin_variables
from get_linkedin_historical_metrics_data import get_linkedin_historical_metrics_data
from get_linkedin_day_before_data import get_linkedin_day_before_data
from get_linkedin_today_data import get_linkedin_today_data
from create_data_for_powerbi import create_linkedin_analytics

# Set starting running date
starting_timestamp = datetime.now()
# print('STARTING DATETIME: ', starting_timestamp, '\n')

# Get linkedin credentials
linkedin_access_token, linkedin_organization_id = get_linkedin_variables()

# Get information about Accounts, Campaign Groups, Campaigns and Ads
names_list = ['Accounts', 'Campaign Groups', 'Campaigns', 'Ads']
data_dfs, data_ids, data_names = main_aux_run(names_list, linkedin_access_token)

# Get data from main_aux_run function to variables
accounts_df, campaign_groups_df, campaigns_df, ads_df = data_dfs
campaign_group_ids, campaign_ids, ad_ids = data_ids
campaign_group_names, campaign_names, ad_names = data_names

# Initialise metrics dataframes
camp_groups_metrics = pd.DataFrame([])
camps_metrics = pd.DataFrame([])
ads_metrics = pd.DataFrame([])

# Get LinkedIn Historical metrics data by adding starting and ending dates
get_historical = True
# get_historical = False
if get_historical:
    camp_groups_metrics, camps_metrics, ads_metrics = get_linkedin_historical_metrics_data(camp_groups_metrics,
                                                                                           camps_metrics,
                                                                                           ads_metrics,
                                                                                           '2022-04-02', '2022-12-31',
                                                                                           campaign_group_ids,
                                                                                           campaign_group_names,
                                                                                           campaign_ids, campaign_names,
                                                                                           ad_ids, ad_names,
                                                                                           linkedin_access_token)

# Get LinkedIn metrics data for the day before
camp_groups_metrics, camps_metrics, ads_metrics = get_linkedin_day_before_data(camp_groups_metrics,
                                                                               camps_metrics,
                                                                               ads_metrics,
                                                                               campaign_group_ids,
                                                                               campaign_group_names,
                                                                               campaign_ids, campaign_names,
                                                                               ad_ids, ad_names,
                                                                               linkedin_access_token)

# Get LinkedIn metrics data for today
camp_groups_metrics, camps_metrics, ads_metrics = get_linkedin_today_data(camp_groups_metrics,
                                                                          camps_metrics,
                                                                          ads_metrics,
                                                                          campaign_group_ids, campaign_group_names,
                                                                          campaign_ids, campaign_names,
                                                                          ad_ids, ad_names,
                                                                          linkedin_access_token)

# Concatenate all files to a single linkedin_ads_data .csv file
linkedin_data = run_concatenate(campaign_groups_df, camp_groups_metrics,
                                campaigns_df, camps_metrics,
                                ads_df, ads_metrics)

# Set ending running date
ending_timestamp = datetime.now()
# print('ENDING DATETIME: ', ending_timestamp, '\n')

# Calculate time needed to run and collect data
total_time_needed = ending_timestamp - starting_timestamp
print('TIME NEEDED: ', total_time_needed)

