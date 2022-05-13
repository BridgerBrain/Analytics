from datetime import datetime, timedelta

from get_linkedin_metrics_data import get_linkedin_metrics_data


def get_linkedin_historical_metrics_data(campaign_groups_metrics, campaigns_metrics, ads_metrics,
                                         initial_date, last_date,
                                         campaign_group_ids, campaign_group_names,
                                         campaign_ids, campaign_names,
                                         ad_ids, ad_names,
                                         linkedin_access_token):
    """ A function that uses a starting and ending date to access historical metrics data for LinkedIn
        Campaign Groups, Campaigns and Ads """

    # Date checks for starting and ending dates
    if type(initial_date) == str:
        initial_date = datetime.strptime(initial_date, '%Y-%m-%d')

    if type(last_date) == str:
        last_date = datetime.strptime(last_date, '%Y-%m-%d')
    last_date += timedelta(days=1)

    # Check if the last_date is greater than today to set a limitation for historical data until day before
    date_today = datetime.now()
    if last_date > date_today:
        last_date = date_today

    for n in range(int((last_date - initial_date).days)):

        ending_date = initial_date + timedelta(n)
        print('Current Historical Date Processing Data: ', (ending_date - timedelta(days=1)).date(), '...')

        # Get Campaign Groups metrics data
        campaign_groups_metrics = get_linkedin_metrics_data(campaign_groups_metrics,
                                                            campaign_group_ids, campaign_group_names,
                                                            'campaign_group',
                                                            ending_date, linkedin_access_token)

        # Get Campaigns metrics data
        campaigns_metrics = get_linkedin_metrics_data(campaigns_metrics,
                                                      campaign_ids, campaign_names,
                                                      'campaign',
                                                      ending_date, linkedin_access_token)

        # Get Ads metrics data
        ads_metrics = get_linkedin_metrics_data(ads_metrics,
                                                ad_ids, ad_names,
                                                'ad',
                                                ending_date, linkedin_access_token)

        # print('\t  Campaign Groups Metrics Dataset Shape: ', campaign_groups_metrics_df.shape)
        # print('\t  Campaigns Metrics Dataset Shape: ', campaigns_metrics_df.shape)
        # print('\t  Ads Metrics Dataset Shape: ', ads_metrics_df.shape)

    print()

    return campaign_groups_metrics, campaigns_metrics, ads_metrics
