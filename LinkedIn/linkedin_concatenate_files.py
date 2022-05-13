import pandas as pd


def run_concatenate(campaign_groups_info, campaign_groups_metrics,
                    campaigns_info, campaigns_metrics,
                    ads_info, ads_metrics):
    """ A function which concatenates the created .csv files for LinkedIn data.
        The function helps us to gather all the available data from Accounts, Campaign Groups, Campaigns and Ads
        from LinkedIn into one .csv file that contains details and metrics data. """

    # Change the type of columns in all dataframes to string as they need to match
    campaign_groups_info = campaign_groups_info.applymap(str)
    campaign_groups_metrics = campaign_groups_metrics.applymap(str)
    campaigns_info = campaigns_info.applymap(str)
    campaigns_metrics = campaigns_metrics.applymap(str)
    ads_info = ads_info.applymap(str)
    ads_metrics = ads_metrics.applymap(str)

    print(' ===> Concatenating LinkedIn Data ... <===')

    # ======================= CAMPAIGN GROUPS =======================
    # Merge Campaign Groups data
    campaign_groups_merged = pd.merge(left=campaign_groups_info, right=campaign_groups_metrics, how='inner')
    # ======================= CAMPAIGN GROUPS =======================

    # ======================= CAMPAIGNS =======================
    # Merge Campaigns data and drop any duplicates
    campaigns_merged = pd.merge(left=campaigns_info, right=campaigns_metrics, how='inner')
    campaigns_merged = campaigns_merged.drop_duplicates()
    # ======================= CAMPAIGNS =======================

    # ======================= ADS =======================
    # Merge Ads data and drop any duplicates
    ads_merged = pd.merge(left=ads_info, right=ads_metrics, on='ad_id')
    ads_merged = ads_merged.drop_duplicates()
    # ======================= ADS =======================

    # ======================= MERGE ALL LINKEDIN DATA =======================
    # Merge Campaign Groups and Campaigns data
    temp_df = pd.merge(left=campaigns_merged, right=campaign_groups_merged, on='campaign_group_id', how='outer')
    temp_df = temp_df[['campaign_group_id', 'campaign_group_name',
                       'campaign_id', 'campaign_name', 'campaign_status']]

    # Merge all LinkedIn data
    linkedin_data = pd.merge(left=temp_df, right=ads_merged, on='campaign_id')

    # Drop any duplicates
    linkedin_data = linkedin_data.drop_duplicates()

    # Reset index of the merged dataframe
    linkedin_data = linkedin_data.reset_index(drop=True)

    # Amend column names and select columns for the new concatenated and finalised dataset
    linkedin_data = linkedin_data.rename(columns={"ad_name_x": "ad_name"})
    linkedin_data = linkedin_data.drop(columns=['ad_name_y'])

    linkedin_data = linkedin_data[['metrics_date', 'ad_created_date', 'ad_last_modified_date',
                                   'account_id',
                                   'campaign_group_id', 'campaign_group_name',
                                   'campaign_id', 'campaign_name',
                                   'ad_id', 'ad_name', 'ad_title',
                                   'campaign_status', 'ad_status', 'ad_type',
                                   'money_spent', 'impressions', 'clicks', 'leads', 'conversions', 'videos_views',
                                   'likes', 'conversions_after_click', 'money_spent_in_usd']]
    # ======================= MERGE ALL LINKEDIN DATA =======================

    print('\t  LinkedIn Ads Dataset Shape:', linkedin_data.shape)
    print(' ===> Concatenating LinkedIn Data ... <===', '\n')

    return linkedin_data
