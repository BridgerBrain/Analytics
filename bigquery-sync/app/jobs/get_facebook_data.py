from google.cloud import bigquery
from datetime import datetime, date, timedelta
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from google.cloud.exceptions import NotFound
from schedule import every, repeat, run_pending
import pandas as pd
from utils import *

def exist_dataset_table(client, table_id, dataset_id, project_id, schema, use_schema,  clustering_fields=None):
    try:
        dataset_ref = "{}.{}".format(project_id, dataset_id)
        client.get_dataset(dataset_ref)  # Make an API request.

    except NotFound:
        dataset_ref = "{}.{}".format(project_id, dataset_id)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        dataset = client.create_dataset(dataset)  # Make an API request.
        logger.info("Created dataset {}.{}".format(client.project, dataset.dataset_id))

    try:
        table_ref = "{}.{}.{}".format(project_id, dataset_id, table_id)
        client.get_table(table_ref)  # Make an API request.

    except NotFound:

        table_ref = "{}.{}.{}".format(project_id, dataset_id, table_id)

        if use_schema:
            table = bigquery.Table(table_ref, schema=schema)

        else:
            table = bigquery.Table(table_ref)

        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date"
        )

        if clustering_fields is not None:
            table.clustering_fields = clustering_fields

        table = client.create_table(table)  # Make an API request.
        logger.info("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

    return 'ok'



def get_facebook_data(APP_ID, APP_SECRET, ACCESS_TOKEN, ACCOUNT_ID, breakdown, date_start):
    try:
        FacebookAdsApi.init(APP_ID, APP_SECRET, ACCESS_TOKEN)

        account = AdAccount('act_' + str(ACCOUNT_ID))
        insights = account.get_insights(fields=[
            AdsInsights.Field.account_id,
            AdsInsights.Field.campaign_id,
            AdsInsights.Field.campaign_name,
            AdsInsights.Field.adset_name,
            AdsInsights.Field.adset_id,
            AdsInsights.Field.ad_name,
            AdsInsights.Field.ad_id,
            AdsInsights.Field.spend,
            AdsInsights.Field.impressions,
            AdsInsights.Field.clicks,
            AdsInsights.Field.actions,
            AdsInsights.Field.conversions,
            AdsInsights.Field.reach,
            AdsInsights.Field.frequency,
            AdsInsights.Field.inline_link_clicks,
            AdsInsights.Field.unique_clicks,

            # AdsInsights.Field.place_page_name,
        ], params={
            'breakdowns': [breakdown],
            'level': 'ad',
            'time_range': {
                'since': date_start,
                'until': date_start
            }, 'time_increment': 1
        })

    except Exception as e:
        logger.critical(e)
        raise

    return insights, breakdown


def convert_to_json_pd(insights, breakdown):
    fb_source = []

    for index, item in enumerate(insights):

        actions = []
        conversions = []

        if 'actions' in item:
            for i, value in enumerate(item['actions']):
                actions.append({'action_type': value['action_type'], 'value': value['value']})

        if 'conversions' in item:
            for i, value in enumerate(item['conversions']):
                conversions.append({'action_type': value['action_type'], 'value': value['value']})

        use_date = datetime.strptime(item['date_start'], '%Y-%m-%d')

        fb_source.append({'date': use_date,
                          'ad_id': item['ad_id'],
                          'ad_name': item['ad_name'],
                          'adset_id': item['adset_id'],
                          'adset_name': item['adset_name'],
                          'campaign_id': item['campaign_id'],
                          'campaign_name': item['campaign_name'],
                          'clicks': item['clicks'],
                          'impressions': item['impressions'],
                          'spend': item['spend'],
                          # 'place_page_name': item['place_page_name'],
                          'country': item['country'],
                          'conversions': conversions,
                          'actions': actions,
                          'reach': item['reach'],
                          'frequency': item['frequency'],
                          'inline_link_clicks': item['inline_link_clicks'],

                          })

    df_facebook = pd.json_normalize(fb_source)
    df_facebook['Breakdown'] = breakdown

    return fb_source, df_facebook


""" Function to prepare Facebook Response for uploading to BigQuery"""


def convert_insights(insights, breakdown):
    df_insights = pd.DataFrame()

    pd_columns = ["ad_id", "ad_name", "adset_id", "adset_name", "campaign_id",
                  "campaign_name", "clicks", "impressions", "spend",
                  "reach", "frequency", "inline_link_clicks", "unique_clicks",
                  "Breakdown_Field", "Breakdown_Value"
                  ]

    for index, item in enumerate(insights):

        data = {i: item[i] for i in item if i not in ['actions', 'conversions', 'date']}
        df_insights_row = pd.DataFrame.from_dict([data])
        list_actions = list()
        list_conversions = list()

        if 'actions' in item:

            list_actions.append(item['actions'])
            df_insights_row['actions'] = list_actions
        else:
            df_insights_row['actions'] = [list_actions]

        if 'conversions' in item:
            list_conversions.append(item['conversions'])
            df_insights_row['conversions'] = list_conversions

        else:
            df_insights_row['conversions'] = [list_conversions]

        dateobj = datetime.strptime(item['date_start'], '%Y-%m-%d')

        df_insights = pd.concat([df_insights, df_insights_row])

    df_insights['date'] = dateobj

    df_insights.reset_index(inplace=True, drop=True)
    df_insights.drop(['date_start', 'date_stop'], inplace=True, axis=1)

    my_floats_raw = ['spend', 'frequency']
    my_floats = [value for value in my_floats_raw if value in data]
    my_integers_raw = ['impressions', 'clicks', 'reach', 'inline_link_clicks', 'unique_clicks']
    my_integers = [value for value in my_integers_raw if value in data]
    my_strings = ['ad_id', 'ad_name', 'adset_id', 'campaign_id', 'campaign_name']
    df_insights[my_floats] = df_insights[my_floats].astype(float)
    df_insights[my_integers] = df_insights[my_integers].astype(int)
    df_insights[my_strings] = df_insights[my_strings].astype(str)

    missing_columns = [i for i in pd_columns if i not in data]

    for other in missing_columns:

        if other in my_floats_raw:

            df_insights[other] = 0
        else:

            if other in my_integers_raw:

                df_insights[other] = 0

            else:

                df_insights[other] = ""

    df_insights['Breakdown_Field'] = breakdown
    df_insights['Breakdown_Value'] = df_insights[breakdown].astype('str')

    df_insights.drop([breakdown], inplace=True, axis=1)
    return df_insights, df_insights.to_json()


@repeat(every().day.at('05:00'))
def main():
    logger.info("Starting Facebook job")
    clustering_fields_facebook = ['campaign_id', 'campaign_name']

    ACCESS_TOKEN = get_environmental_variable("FB_ACCESS_TOKEN")
    APP_SECRET = get_environmental_variable("FB_APP_SECRET")
    APP_ID = get_environmental_variable("FB_APP_ID")
    ACCOUNT_ID = get_environmental_variable("FB_ACCOUNT_ID")
    FacebookAdsApi.init(access_token=ACCESS_TOKEN)

    breakdown_list = ["country"]

    schema_facebook_stat = [
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("ad_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("ad_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("adset_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("adset_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("campaign_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("campaign_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("clicks", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("impressions", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("spend", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("reach", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("frequency", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("inline_link_clicks", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("unique_clicks", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField('conversions', 'RECORD', mode='REPEATED',
                             fields=(bigquery.SchemaField('action_type', 'STRING'),
                                     bigquery.SchemaField('value', 'STRING'))),
        bigquery.SchemaField('actions', 'RECORD', mode='REPEATED',
                             fields=(bigquery.SchemaField('action_type', 'STRING'),
                                     bigquery.SchemaField('value', 'STRING'))),
        bigquery.SchemaField("Breakdown_Field", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Breakdown_Value", "STRING", mode="REQUIRED"),

    ]

    for breakdown in breakdown_list:
        # breakdown = breakdown_list[0]
        logger.info('Obtaining Facebook Metrics by Breakdown <{}>...'.format(breakdown))
        dict_dataframes = dict()

        date_today = date.today()
        date_start = date_today - timedelta(days=1)

        date_string = str(date_start)

        logger.info('Obtaining Facebook Metrics for date <{}>...'.format(date_string))

        [insights, breakdown] = get_facebook_data(APP_ID, APP_SECRET, ACCESS_TOKEN, ACCOUNT_ID, breakdown, date_string)

        if len(insights) > 0:
            [df_fb, fb_jason] = convert_insights(insights, breakdown)

            dict_dataframes[breakdown] = df_fb

            df_new = df_fb.drop(['account_id'], axis=1)
            client = bigquery.Client()
            table_id = 'fb_marketing'
            dataset_id = 'facebook_ads'
            project_id = get_environmental_variable("GOOGLE_PROJECT_ID")
            full_table_id = "{}.{}.{}".format(project_id, dataset_id, table_id)
            job_config = bigquery.LoadJobConfig(schema=schema_facebook_stat, write_disposition="WRITE_APPEND")
            # job_config = bigquery.LoadJobConfig( write_disposition="WRITE_APPEND")
            use_schema = True

            if exist_dataset_table(client, table_id, dataset_id, project_id, schema_facebook_stat, use_schema, clustering_fields_facebook) == 'ok':
                logger.warning('Uploading Facebook Data for Date <{}> to BigQuery'.format(date_string))
                job = client.load_table_from_dataframe(
                    df_new, full_table_id, job_config=job_config)
        else:
            logger.info('(There is no response for Breakdown <{}>'.format(breakdown))
    logger.info("Done Facebook job")
