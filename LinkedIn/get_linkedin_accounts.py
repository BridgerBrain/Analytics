import requests
import json
import pandas as pd
from datetime import datetime
# blslbsldgswlgtnwgt3wgw
def get_linkedin_accounts(access_token):
    """ A function which access LinkedIn Accounts and gives us information about them """

    accounts_list = []
    accounts_df = pd.DataFrame([])

    # Define URL API
    url = 'https://api.linkedin.com/v2/adAccountsV2'
    url += '?q=search&search.type.values[0]=BUSINESS&search.status.values[0]=ACTIVE'

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

        # Access Accounts' information
        if 'elements' in response_dict:

            accounts = response_dict['elements']

            for temp_acc in accounts:

                temp_account_id = temp_acc['id']
                temp_account_name = temp_acc['name']
                temp_account_type = temp_acc['type']

                temp_page_urn = temp_acc['reference']
                temp_page_id = temp_page_urn[-8:]

                temp_account_status = temp_acc['status']
                temp_account_currency = temp_acc['currency']

                temp_account_serving_statuses = temp_acc['servingStatuses']
                temp_account_version = temp_acc['version']['versionTag']
                temp_account_test = temp_acc['test']

                temp_dates = temp_acc['changeAuditStamps']
                temp_created_date = datetime.fromtimestamp(temp_dates['created']['time'] / 1e3)
                temp_last_modified_date = datetime.fromtimestamp(temp_dates['lastModified']['time'] / 1e3)

                temp_account_notified_on_rejection = temp_acc['notifiedOnCreativeRejection']
                temp_account_notified_on_end_campaign = temp_acc['notifiedOnEndOfCampaign']
                temp_account_notified_on_campaign_opt = temp_acc['notifiedOnCampaignOptimization']
                temp_account_notified_on_creative_approval = temp_acc['notifiedOnCreativeApproval']

                # Save Accounts details to a list, then to a dataframe and save the table as a .csv file
                accounts_list.append([temp_account_id, temp_account_name, temp_account_type,
                                      temp_page_id,
                                      temp_account_status, temp_account_currency,
                                      temp_account_serving_statuses, temp_account_version, temp_account_test,
                                      temp_created_date, temp_last_modified_date,
                                      temp_account_notified_on_rejection, temp_account_notified_on_end_campaign,
                                      temp_account_notified_on_campaign_opt,
                                      temp_account_notified_on_creative_approval])

                accounts_df = pd.DataFrame(accounts_list,
                                           columns=['account_id', 'account_name', 'account_type',
                                                    'page_id',
                                                    'account_status', 'account_currency',
                                                    'account_serving_statuses', 'account_version', 'is_account_test',
                                                    'created_date', 'last_modified_date',
                                                    'notified_on_rejection', 'notified_on_end_campaign',
                                                    'notified_on_campaign_optimization',
                                                    'notified_on_creative_approval'])

    return accounts_df
