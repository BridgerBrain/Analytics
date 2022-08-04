from BQ_functions import get_bigquery_table, upload_table_to_bigquery
from data_collection import data_collection
from aux_functions import create_closed_won_data, create_aggregated_data, get_summarised_data
from arima_predictions import run_ARIMA

import logging as logger

import warnings
warnings.filterwarnings("ignore")

project_id, QUERY = data_collection()

performance_marketing_data = get_bigquery_table(QUERY, project_id)

data = create_closed_won_data(performance_marketing_data)

data = create_aggregated_data(data)

data_2021 = data[data.index < "2022-01-01"]
data_2022 = data[data.index >= "2022-01-01"]


# ============================== ARIMA ===================================
# ========================================================================
ARIMA_data, ARIMA_predictions, ARIMA_conf_int_pred, metrics_list_ARIMA, ARIMA_parameters_set = run_ARIMA(data_2022,
                                                                                                         [3, 0, 5],
                                                                                                         False)
ARIMA_MSE_score, ARIMA_r2_score = metrics_list_ARIMA

logger.info("ARIMA(3, 0, 5): \n"
            "\t \t MSE SCORE =       {} \n"
            "\t \t R2 SCORE SCORE =  {} \n".format(ARIMA_MSE_score, ARIMA_r2_score))
print("ARIMA(3, 0, 5): \n"
      "\t \t MSE SCORE =       {} \n"
      "\t \t R2 SCORE SCORE =  {} \n".format(ARIMA_MSE_score, ARIMA_r2_score))
# ========================================================================
# ========================================================================

summarised_data = get_summarised_data(data, ARIMA_predictions)

# ============================== TABLES TO BIG QUERY ===================================
# ======================================================================================

upload_table_to_bigquery(summarised_data, 'Summarised_Table', bool_upload=True)

performance_marketing_data = get_bigquery_table(QUERY, project_id)
performance_marketing_data.Lead_ID = performance_marketing_data.Lead_ID.astype(str)
performance_marketing_data.Accounts_Adjusted_Account_ID = performance_marketing_data.Accounts_Adjusted_Account_ID.astype(str)
performance_marketing_data.Deals_Adjusted_Deal_ID = performance_marketing_data.Deals_Adjusted_Deal_ID.astype(str)
upload_table_to_bigquery(performance_marketing_data, 'Performance_Marketing_Report', bool_upload=True)

# ======================================================================================
# ======================================================================================

