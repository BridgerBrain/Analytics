import requests
import json
import pandas as pd
from datetime import datetime

from get_linkedin_ad_details import get_linkedin_ad_details


def get_linkedin_ads(account_id, access_token):
    """ A function which access LinkedIn Ads and gives us information about them """

    ads_metrics_list = []
    ads_metrics_df = pd.DataFrame([])

    # Define headers
    headers = {'Authorization': 'Bearer ' + access_token}

    # Define URL API
    url = 'https://api.linkedin.com/v2/adCreativesV2'
    url += '?q=search&search.account.values[0]=urn:li:sponsoredAccount:'
    url += str(account_id)

    # Make the http call
    r = requests.get(url=url, headers=headers)
    ads_response_dict = json.loads(r.text)

    dict_elements = ads_response_dict['elements']

    # Access Ads information
    for i in range(len(dict_elements)):

        temp_dict = dict_elements[i]

        temp_account_id = account_id
        temp_campaign_id = temp_dict['campaign'][-9:]
        temp_ad_id = temp_dict['id']

        temp_ad_reference = temp_dict['reference'][-19:]

        # Access Ad details using a function
        temp_ad_title, temp_ad_name = get_linkedin_ad_details(temp_ad_id, temp_ad_reference, access_token)

        temp_ad_status = temp_dict['status']
        temp_ad_type = temp_dict['type']
        temp_ad_test = temp_dict['test']
        temp_ad_version = temp_dict['version']['versionTag']

        if 'callToAction' in temp_dict.keys():
            temp_call_to_action = temp_dict['callToAction']['labelType']
        else:
            temp_call_to_action = None

        if len(temp_dict['variables']['data'].keys()) != 0:
            temp_ad_other = list(temp_dict['variables']['data'].keys())[0][17:]
        else:
            temp_ad_other = None

        if temp_ad_other is not None:
            if 'Carousel' in temp_ad_other:
                temp_ad_name = 'Gen_Carousel_A1'

            if temp_ad_title is None and 'VideoCreative' in temp_ad_other:
                temp_ad_title = 'Get started in a few clicks'

        temp_ad_dates = temp_dict['changeAuditStamps']
        temp_ad_created_date = datetime.fromtimestamp(temp_ad_dates['created']['time'] / 1e3)
        temp_ad_last_modified_date = datetime.fromtimestamp(temp_ad_dates['lastModified']['time'] / 1e3)

        # Save Ads details to a list, then to a dataframe and save the table as a .csv file
        ads_metrics_list.append([temp_account_id, temp_campaign_id, temp_ad_id,
                                 temp_ad_name, temp_ad_title,
                                 temp_ad_status, temp_ad_type, temp_ad_other, temp_call_to_action,
                                 temp_ad_test, temp_ad_version, temp_ad_reference,
                                 temp_ad_created_date, temp_ad_last_modified_date])

        ads_metrics_df = pd.DataFrame(ads_metrics_list,
                                      columns=['account_id', 'campaign_id', 'ad_id',
                                               'ad_name', 'ad_title',
                                               'ad_status', 'ad_type', 'ad_other', 'call_to_action',
                                               'is_ad_test', 'ad_version', 'ad_reference',
                                               'ad_created_date', 'ad_last_modified_date'])

    return ads_metrics_df
