from datetime import datetime, timedelta
from get_linkedin_metrics_data import get_linkedin_metrics_data


def get_linkedin_day_before_data(campaign_groups_metrics, campaigns_metrics, ads_metrics,
                                 campaign_group_ids, campaign_group_names,
                                 campaign_ids, campaign_names,
                                 ad_ids, ad_names,
                                 linkedin_access_token):
    """ A function that uses tomorrow's date to access LinkedIn metrics data from today.
        It helps us either to update the historical files or to create a new .csv file containing only
        the available metrics data about LinkedIn from the day before """

    # Access date today
    today_date = datetime.now()
    print('Current Date Processing Data: ', (today_date - timedelta(days=1)).date(), '...')

    # Get metrics for Campaign Groups
    campaign_groups_metrics = get_linkedin_metrics_data(campaign_groups_metrics,
                                                        campaign_group_ids, campaign_group_names,
                                                        'campaign_group',
                                                        today_date, linkedin_access_token)

    # Get metrics for Campaigns
    campaigns_metrics = get_linkedin_metrics_data(campaigns_metrics,
                                                  campaign_ids, campaign_names,
                                                  'campaign',
                                                  today_date, linkedin_access_token)

    # Get metrics for Ads
    ads_metrics = get_linkedin_metrics_data(ads_metrics,
                                            ad_ids, ad_names,
                                            'ad',
                                            today_date, linkedin_access_token)

    # print('\t  Campaign Groups Metrics Dataset Shape: ', campaign_groups_metrics_df.shape)
    # print('\t  Campaigns Metrics Dataset Shape: ', campaigns_metrics_df.shape)
    # print('\t  Ads Metrics Dataset Shape: ', ads_metrics_df.shape, '\n')

    print()

    return campaign_groups_metrics, campaigns_metrics, ads_metrics
