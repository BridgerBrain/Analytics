import pandas as pd


def create_linkedin_analytics(data):

    channel_type = pd.Series([0] * len(data))
    marketing_source = pd.Series(['LinkedIn'] * len(data))

    data1 = data[['clicks', 'money_spent_in_usd', 'impressions', 'campaign_id', 'campaign_name',
                 'campaign_group_id']]
    data2 = data[['campaign_group_name', 'conversions', 'ad_id', 'metrics_date']]
    data3 = data[['ad_id']]

    data = pd.concat([data1, channel_type, data2, marketing_source, data3], axis=1)

    data.columns = ['clicks', 'Cost_USD', 'impressions', 'adset_id', 'adset_name',
                    'campaign_id', 'Channel_Type', 'campaign_name', 'Conversions',
                    'ad_id', 'date', 'Marketing_Source', 'ad_name']

    data.to_csv('LinkedIn_Analytics.csv', index=False)

    return data


