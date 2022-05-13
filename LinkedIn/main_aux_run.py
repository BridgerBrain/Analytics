from get_linkedin_accounts import get_linkedin_accounts
from get_linkedin_campaign_groups import get_linkedin_campaign_groups
from get_linkedin_campaigns import get_linkedin_campaigns
from get_linkedin_ads import get_linkedin_ads


def main_aux_run(names_list, linkedin_access_token):
    """ An auxiliary function which helps main.py file run functions to get information about
        LinkedIn Accounts, Campaign Groups, Campaigns and Ads and save those dataframes as .csv files """

    # Get Accounts information
    print(' ===> LinkedIn ' + names_list[0] + ' Data Extraction Process Starts ... <===')
    accounts_df = get_linkedin_accounts(linkedin_access_token)
    linkedin_account_ids = list(accounts_df.account_id.iloc)
    print('\t  Total ' + names_list[0] + ':        ', len(linkedin_account_ids))
    print('\t  ' + names_list[0][:-1] + ' IDs:           ', linkedin_account_ids)
    print(' ===> LinkedIn ' + names_list[0] + ' Data Extraction Process Finished <=== \n')

    # Get Campaign Groups information
    print(' ===> LinkedIn ' + names_list[1] + ' Data Extraction Process Starts ... <===')
    campaign_groups_df = get_linkedin_campaign_groups(linkedin_account_ids[0], linkedin_access_token)
    campaign_group_ids = list(campaign_groups_df.campaign_group_id)
    campaign_group_names = list(campaign_groups_df.campaign_group_name)
    print('\t  Total ' + names_list[1] + ': ', len(campaign_group_ids))
    print('\t  ' + names_list[1][:-1] + ' IDs:    ', campaign_group_ids)
    print('\t  ' + names_list[1][:-1] + ' Names:  ', campaign_group_names)
    print(' ===> LinkedIn ' + names_list[1] + ' Data Extraction Process Finished <=== \n')

    # Get Campaigns information
    print(' ===> LinkedIn ' + names_list[2] + ' Data Extraction Process Starts ... <===')
    campaigns_df = get_linkedin_campaigns(campaign_group_ids, linkedin_account_ids, linkedin_access_token)
    campaign_ids = list(campaigns_df.campaign_id)
    campaign_names = list(campaigns_df.campaign_name)
    print('\t  Total ' + names_list[2] + ':       ', len(campaign_ids))
    print('\t  ' + names_list[2][:-1] + '  IDs:         ', campaign_ids)
    print('\t  ' + names_list[2][:-1] + '  Names:       ', campaign_names)
    print(' ===> LinkedIn ' + names_list[2] + ' Data Extraction Process Finished <=== \n')

    # Get Ads information
    print(' ===> LinkedIn ' + names_list[3] + ' Data Extraction Process Starts ... <===')
    ads_df = get_linkedin_ads(linkedin_account_ids[0], linkedin_access_token)
    ad_ids = list(ads_df.ad_id)
    ad_names = list(ads_df.ad_title)
    print('\t  Total ' + names_list[3] + ':             ', len(ad_ids))
    print('\t  ' + names_list[3][:-1] + ' IDs:                ', ad_ids)
    print('\t  ' + names_list[3][:-1] + ' Names:              ', ad_names)
    print(' ===> LinkedIn ' + names_list[3] + ' Data Extraction Process Finished <=== \n')

    data_dfs = [accounts_df, campaign_groups_df, campaigns_df, ads_df]
    data_ids = [campaign_group_ids, campaign_ids, ad_ids]
    data_names = [campaign_group_names, campaign_names, ad_names]

    return data_dfs, data_ids, data_names
