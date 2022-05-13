import pandas as pd
from datetime import datetime


def get_campaigns(resp_dict):
    """ A function which reads a dictionary with Campaigns information and values and returns a dataframe containing
        all the required details about the available Campaigns """

    campaigns_info_list = []

    for k in range(len(resp_dict['elements'])):

        campaigns_dict = resp_dict['elements'][k]

        temp_account_id = campaigns_dict['account'][-9:]
        temp_campaign_group = campaigns_dict['campaignGroup'][-9:]
        temp_page_id = campaigns_dict['associatedEntity'][-8:]

        temp_camp_id = campaigns_dict['id']
        temp_camp_name = campaigns_dict['name']
        temp_camp_status = campaigns_dict['status']
        temp_camp_cost_type = campaigns_dict['costType']
        temp_camp_type = campaigns_dict['type']
        temp_camp_format = campaigns_dict['format']

        temp_camp_daily_budget_currency = campaigns_dict['dailyBudget']['currencyCode']
        temp_camp_daily_budget_amount = campaigns_dict['dailyBudget']['amount']

        temp_camp_unit_cost_currency = campaigns_dict['unitCost']['currencyCode']
        temp_camp_unit_cost_amount = campaigns_dict['unitCost']['amount']

        temp_camp_objective_type = campaigns_dict['objectiveType']
        temp_camp_creative_selection = campaigns_dict['creativeSelection']

        if 'pacingStrategy' in campaigns_dict.keys():
            temp_camp_pacing_strategy = campaigns_dict['pacingStrategy']
        else:
            temp_camp_pacing_strategy = None

        temp_camp_optimization_target_type = campaigns_dict['optimizationTargetType']

        temp_camp_test = campaigns_dict['test']

        temp_camp_serving_statuses = campaigns_dict['servingStatuses']
        temp_camp_version = campaigns_dict['version']['versionTag']

        temp_camp_dates = campaigns_dict['changeAuditStamps']
        temp_camp_created_date = datetime.fromtimestamp(temp_camp_dates['created']['time'] / 1e3)
        temp_camp_last_modified_date = datetime.fromtimestamp(temp_camp_dates['lastModified']['time'] / 1e3)

        temp_camp_started_running_date = datetime.fromtimestamp(campaigns_dict['runSchedule']['start'] / 1e3)

        # Save Campaigns details to a list and then to a dataframe
        campaigns_info_list.append([temp_account_id, temp_page_id, temp_campaign_group,
                                    temp_camp_id, temp_camp_name, temp_camp_status,
                                    temp_camp_cost_type, temp_camp_type, temp_camp_format,
                                    temp_camp_daily_budget_amount, temp_camp_daily_budget_currency,
                                    temp_camp_unit_cost_amount, temp_camp_unit_cost_currency,
                                    temp_camp_objective_type, temp_camp_creative_selection,
                                    temp_camp_pacing_strategy, temp_camp_optimization_target_type,
                                    temp_camp_test, temp_camp_serving_statuses, temp_camp_version,
                                    temp_camp_created_date, temp_camp_last_modified_date,
                                    temp_camp_started_running_date])

    all_campaigns_info_df = pd.DataFrame(campaigns_info_list,
                                         columns=['account_id', 'page_id', 'campaign_group_id',
                                                  'campaign_id', 'campaign_name', 'campaign_status',
                                                  'campaign_cost_type', 'campaign_type', 'campaign_format',
                                                  'campaign_daily_budget_amount', 'campaign_daily_budget_currency',
                                                  'campaign_unit_cost_amount', 'campaign_unit_cost_currency',
                                                  'campaign_objective_type', 'campaign_creative_selection',
                                                  'campaign_pacing_strategy', 'campaign_optimization_target_type',
                                                  'is_campaign_test', 'campaign_serving_statuses', 'campaign_version',
                                                  'campaign_created_date', 'campaign_last_modified_date',
                                                  'campaign_started_running_date'])

    return all_campaigns_info_df
