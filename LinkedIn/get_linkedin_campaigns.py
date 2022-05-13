import pandas as pd
import requests
import json

from get_linkedin_campaigns_aux import get_campaigns


def get_linkedin_campaigns(campaign_group_ids, account_ids, access_token):
    """ A function which access LinkedIn Campaigns and gives us information about them """

    total_runs = 0
    campaigns_df = pd.DataFrame([])

    # If a valid campaign_group_ids list is given, then Campaign details are collected by using campaign_group_ids
    if campaign_group_ids is not None and total_runs == 0:

        for camp_group_id in campaign_group_ids:

            # Define URL API
            url = 'https://api.linkedin.com/v2/adCampaignsV2'
            url += '?q=search&search.campaignGroup.values[0]=urn:li:sponsoredCampaignGroup:'
            url += str(camp_group_id)

            # Define headers
            headers = {'Authorization': 'Bearer ' + access_token}

            # Make the http call
            r = requests.get(url=url, headers=headers)
            campaigns_group_response_dict = json.loads(r.text)

            # Get Campaign details in a dataframe using a function
            # campaigns_df = get_campaigns(campaigns_group_response_dict)
            temp_campaigns_df = get_campaigns(campaigns_group_response_dict)
            campaigns_df = pd.concat([temp_campaigns_df, campaigns_df])

        total_runs += 1

    # If campaign_group_ids list is empty, then Campaigns details are collected using account_ids
    if account_ids is not None and total_runs == 0:

        for account_id in account_ids:

            # Define URL API
            url = 'https://api.linkedin.com/v2/adCampaignsV2'
            url += '?q=search&search.account.values[0]=urn:li:sponsoredAccount:'
            url += str(account_id)

            # Define headers
            headers = {'Authorization': 'Bearer ' + access_token}

            # Make the http call
            r = requests.get(url=url, headers=headers)
            accounts_response_dict = json.loads(r.text)

            # Get Campaign details in a dataframe using a function
            campaigns_df = get_campaigns(accounts_response_dict)

        total_runs += 1

    if total_runs == 0:
        print('Enter a valid list with Campaigns Group IDs ids OR Accounts IDs...')

    return campaigns_df

