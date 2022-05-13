import requests
import json
import pandas as pd
from datetime import datetime


def get_linkedin_campaign_groups(account_id, access_token):
    """ A function which access LinkedIn Campaign Groups and gives us information about them """

    campaign_groups_list = []
    campaign_groups_df = pd.DataFrame([])

    # Define URL API
    url = 'https://api.linkedin.com/v2/adCampaignGroupsV2'
    url += '?q=search&search.account.values[0]=urn:li:sponsoredAccount:'
    url += str(account_id)

    # Define headers
    headers = {'Authorization': 'Bearer ' + access_token}

    # Make the http call
    r = requests.get(url=url, headers=headers)

    # Check if requests have a non valid error code
    if r.status_code != 200:
        print('\n ### something went wrong ### ', r, '\n')

    else:
        # Access request results
        response_dict = json.loads(r.text)

        # Access Campaign Groups' information
        if 'elements' in response_dict:

            campaign_groups = response_dict['elements']

            for camp_group in campaign_groups:

                temp_account_urn = camp_group['account']
                temp_account_id = temp_account_urn[-9:]

                temp_camp_group_id = camp_group['id']
                temp_camp_group_name = camp_group['name']
                temp_camp_group_status = camp_group['status']

                temp_camp_group_serving_statuses = camp_group['servingStatuses']
                temp_test = camp_group['test']
                temp_backfilled = camp_group['backfilled']

                temp_camp_group_dates = camp_group['changeAuditStamps']
                temp_camp_group_created_date = datetime.fromtimestamp(temp_camp_group_dates['created']['time'] / 1e3)
                temp_camp_group_last_modified_date = datetime.fromtimestamp(temp_camp_group_dates['lastModified']['time'] / 1e3)

                temp_camp_group_running_date = datetime.fromtimestamp(camp_group['runSchedule']['start'] / 1e3)

                # Calculate the number of Campaigns in the Campaign Group
                new_url = 'https://api.linkedin.com/v2/adCampaignsV2'
                new_url += '?q=search&search.campaignGroup.values[0]=urn:li:sponsoredCampaignGroup:'
                new_url += str(temp_camp_group_id)

                temp_r = requests.get(url=new_url, headers=headers)
                temp_resp_dict = json.loads(temp_r.text)

                temp_total_campaigns = len(temp_resp_dict['elements'])

                # Save Campaign Groups details to a list, then to a dataframe and save the table as a .csv file
                campaign_groups_list.append([temp_account_id,
                                             temp_camp_group_id, temp_camp_group_name,
                                             temp_total_campaigns,
                                             temp_camp_group_status, temp_camp_group_serving_statuses,
                                             temp_test, temp_backfilled,
                                             temp_camp_group_created_date, temp_camp_group_last_modified_date,
                                             temp_camp_group_running_date])

                campaign_groups_df = pd.DataFrame(campaign_groups_list,
                                                  columns=['account_id',
                                                           'campaign_group_id', 'campaign_group_name',
                                                           'total_campaigns',
                                                           'campaign_group_status', 'campaign_group_serving_statuses',
                                                           'is_campaign_group_test', 'is_campaign_group_backfilled',
                                                           'campaign_group_created_date',
                                                           'campaign_group_last_modified_date',
                                                           'campaign_group_started_running_date'])

    return campaign_groups_df
