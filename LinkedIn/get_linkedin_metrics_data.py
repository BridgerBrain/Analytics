import requests
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


def get_linkedin_metrics_data(temp_metrics_df, temp_ids, temp_names, temp_text,
                              data_date, access_token):

    """ A function which access metrics data for LinkedIn Campaign Groups, Campaigns and Ads and helps us to store
        all the available information about them """

    # Set as metrics date the day before
    metrics_date = data_date - timedelta(days=1)
    metrics_date = datetime.strftime(metrics_date, '%Y-%m-%d')

    # Split metrics date to year, month and day
    metrics_year = metrics_date[:4]
    metrics_month = metrics_date[5:7]
    metrics_day = metrics_date[8:10]

    metrics_list = []
    metrics_df = temp_metrics_df

    if temp_text == 'campaign_group':
        url = 'CAMPAIGN_GROUP'
    elif temp_text == 'campaign':
        url = 'CAMPAIGN'
    elif temp_text == 'ad':
        url = 'CREATIVE'
    else:
        url = None

    temp_id_text = temp_text + '_id'
    temp_name_text = temp_text + '_name'

    temp_url = '&accounts=urn%3Ali%3AsponsoredAccount%3A508312367&pivot=' + url

    # Make the http call
    headers = {'Authorization': 'Bearer ' + access_token}

    # Define metrics we want to access
    metric_field_names = 'externalWebsiteConversions,externalWebsitePostClickConversions,costInLocalCurrency,costInUsd,'
    metric_field_names += 'impressions,likes,clicks,videoViews,oneClickLeads'

    # Define URL API by setting the starting and ending dates to metrics date and get the daily metrics
    url = 'https://api.linkedin.com/v2/adAnalyticsV2?q=analytics'
    url += '&dateRange.start.year=' + metrics_year
    url += '&dateRange.start.month=' + metrics_month
    url += '&dateRange.start.day=' + metrics_day
    url += '&dateRange.end.year=' + metrics_year
    url += '&dateRange.end.month=' + metrics_month
    url += '&dateRange.end.day=' + metrics_day
    url += '&timeGranularity=DAILY'
    url += temp_url
    url += '&projection=(*,elements*(pivot,pivotValue~(name),dateRange(*),' + metric_field_names + '))'
    url += '&fields=pivot,pivotValue,dateRange,' + metric_field_names

    # Make the http call
    r = requests.get(url=url, headers=headers)
    response_dict = json.loads(r.text)


    # # Check if *_metrics.csv file already exists in the datasets' directory, otherwise create a new dataframe and save
    # # it as a .csv file to the directory
    # if os.path.isfile('datasets/metrics/' + file_name + '_metrics.csv'):
    #     metrics_df = pd.read_csv('datasets/metrics/' + file_name + '_metrics.csv')
    # else:
    #     metrics_df = pd.DataFrame([])
    #     metrics_df.to_csv('datasets/metrics/' + file_name + '_metrics.csv')

    # # Initiate a dataframe to store metrics data
    # metrics_df = pd.DataFrame([],
    #                           columns=['metrics_date', temp_id_text, temp_name_text, 'money_spent',
    #                                    'impressions', 'clicks', 'leads', 'conversions', 'videos_views',
    #                                    'likes', 'conversions_after_click', 'money_spent_in_usd'])

    # Check if metrics date is already in the metrics dataframe and if not access and store metrics data
    if metrics_df.shape[0] > 0 and metrics_date in list(metrics_df['metrics_date']):
        pass
    else:

        for num in range(len(temp_ids)):

            temp_name = temp_names[num]
            temp_id = temp_ids[num]

            total_conversions = 0
            total_conversions_after_click = 0
            total_spent = 0
            total_spent_in_usd = 0

            total_impressions = 0
            total_likes = 0
            total_clicks = 0
            total_video_view = 0
            total_leads = 0

            for i in range(len(response_dict['elements'])):

                temp_id_dict = int(response_dict['elements'][i]['pivotValue'][-9:])

                if temp_id == temp_id_dict:

                    total_conversions += response_dict['elements'][i]['externalWebsiteConversions']
                    total_conversions_after_click += response_dict['elements'][i]['externalWebsitePostClickConversions']
                    total_spent += float(response_dict['elements'][i]['costInLocalCurrency'])
                    total_spent_in_usd += float(response_dict['elements'][i]['costInUsd'])

                    total_impressions += response_dict['elements'][i]['impressions']
                    total_likes += response_dict['elements'][i]['likes']
                    total_clicks += response_dict['elements'][i]['clicks']
                    total_video_view += response_dict['elements'][i]['videoViews']
                    total_leads += response_dict['elements'][i]['oneClickLeads']

            total_spent = np.round(total_spent, 2)
            total_spent_in_usd = np.round(total_spent_in_usd, 2)

            # Save metrics details to a list, then to a dataframe and save the table as a .csv file
            metrics_list.append([metrics_date, temp_id, temp_name, total_spent,
                                 total_impressions, total_clicks, total_leads, total_conversions, total_video_view,
                                 total_likes, total_conversions_after_click, total_spent_in_usd])

            temp_metrics_df = pd.DataFrame(metrics_list,
                                           columns=['metrics_date', temp_id_text, temp_name_text, 'money_spent',
                                                    'impressions', 'clicks', 'leads', 'conversions', 'videos_views',
                                                    'likes', 'conversions_after_click', 'money_spent_in_usd'])

        metrics_df = pd.concat([metrics_df, temp_metrics_df], axis=0)
        metrics_df = metrics_df.drop_duplicates()

    return metrics_df
