import numpy as np
import pandas as pd
from datetime import datetime, date


def id_type_fun(dataset):
    if pd.isna(dataset.Accounts_Adjusted_Account_ID) is False and pd.isna(dataset.Lead_ID) is True:
        return "Account"
    elif pd.isna(dataset.Converted_Account_ID) is False:
        return "Account"
    elif pd.isna(dataset.Deals_Adjusted_Deal_ID) is False \
            and pd.isna(dataset.Converted_Account_ID) is True \
            and pd.isna(dataset.Lead_ID) is True \
            and pd.isna(dataset.Accounts_Adjusted_Account_ID) is True:
        return "Deal"
    else:
        return "Lead"


def generic_id_fun(dataset):
    if pd.isna(dataset.Lead_ID) is True and pd.isna(dataset.Accounts_Adjusted_Account_ID) is False:
        return dataset.Accounts_Adjusted_Account_ID
    elif pd.isna(dataset.Converted_Account_ID) is False:
        return dataset.Converted_Account_ID
    else:
        return dataset.Lead_ID


def new_lead_status_fun(dataset):
    if dataset.ID_Type == "Lead":
        return dataset.Lead_Status
    elif dataset.ID_Type == "Account" and pd.isna(dataset.Accounts_Adjusted_Lead_Status) is True:
        return dataset.Lead_Status
    else:
        return dataset.Accounts_Adjusted_Lead_Status


def ignore_rows_fun(dataset):
    if dataset.New_Lead_Status == "Duplicate":
        return 1
    else:
        return 0


def demo_booked_fun(dataset):
    if dataset.Bool_Ignore_Rows == 0 and \
            (pd.isna(dataset.Demo_Date) is False
             or dataset.Demo_Status == "Demo Booked"
             or dataset.Demo_Status == "Demo Done"
             or pd.isna(dataset.Accounts_Adjusted_Demo_Date) is False
             or dataset.Accounts_Adjusted_Demo_Status == "Demo Booked"
             or dataset.Accounts_Adjusted_Demo_Status == "Demo Done"):
        return 1
    else:
        return 0


def my_subscription_plan_fun(dataset):
    if pd.isna(dataset.Accounts_Adjusted_Subscription_Plan) is True \
            and pd.isna(dataset.Deals_Adjusted_Subscription_Plan) is False:
        return dataset.Deals_Adjusted_Subscription_Plan
    elif pd.isna(dataset.Accounts_Adjusted_Subscription_Plan) is False \
            and pd.isna(dataset.Deals_Adjusted_Subscription_Plan) is True:
        return dataset.Accounts_Adjusted_Subscription_Plan
    elif pd.isna(dataset.Accounts_Adjusted_Subscription_Plan) is False \
            and pd.isna(dataset.Deals_Adjusted_Subscription_Plan) is False:
        return dataset.Deals_Adjusted_Subscription_Plan
    else:
        return None


def closed_won_fun(dataset):
    if dataset.Bool_Ignore_Rows == 0 \
            and (dataset.Deals_Adjusted_Stage == "Contract Signed - Payment not received"
                 or dataset.Deals_Adjusted_Stage == "Closed Won - Payment Received") \
            and dataset.My_Subscription_Plan != "Free" \
            and dataset.My_Subscription_Plan != "-" \
            and pd.isna(dataset.My_Subscription_Plan) is False:
        return 1
    else:
        return 0


def closed_won_date_fun(dataset):
    if dataset.Deals_Adjusted_Stage == "Contract Signed - Payment not received" \
            and dataset.Generic_ID == "4449376000043190397":
        return datetime.strptime('2022-07-14', '%Y-%m-%d')
    elif dataset.Deals_Adjusted_Stage == "Contract Signed - Payment not received" \
            and dataset.Generic_ID == "4449376000036892030":
        return datetime.strptime('2022-07-20', '%Y-%m-%d')
    elif dataset.Deals_Adjusted_Stage == "Contract Signed - Payment not received" \
            and dataset.Generic_ID == "4449376000052948417":
        return datetime.strptime('2022-07-26', '%Y-%m-%d')
    elif dataset.Bool_Closed_Won == 1 \
            and pd.isna(dataset.Deals_Adjusted_Closing_Date) is False \
            and dataset.Deals_Adjusted_Stage == "Closed Won - Payment Received":
        return datetime.strptime(dataset.Deals_Adjusted_Closing_Date, '%Y-%m-%d')
    elif dataset.Bool_Ignore_Rows == 1:
        return None
    else:
        return None


def my_date_created_fun(dataset):
    if dataset.Bool_Ignore_Rows == 1:
        return None
    elif dataset.ID_Type == "Lead":
        return datetime.strptime(dataset.Created_Time[:10], '%Y-%m-%d')
    elif dataset.ID_Type == "Account" and pd.isna(dataset.Lead_ID) is False and pd.isna(dataset.Created_Time) is False:
        return datetime.strptime(dataset.Created_Time[:10], '%Y-%m-%d')
    elif dataset.ID_Type == "Account" and pd.isna(dataset.Lead_ID) is False and pd.isna(dataset.Created_Time) is True:
        return datetime.strptime(dataset.Accounts_Adjusted_Created_Time[:10], '%Y-%m-%d')
    elif dataset.ID_Type == "Account" and pd.isna(dataset.Lead_ID) is True:
        return datetime.strptime(dataset.Accounts_Adjusted_Created_Time[:10], '%Y-%m-%d')


def closed_won_same_month_fun(dataset):
    if dataset.Bool_Closed_Won == 1 \
            and dataset.My_Date_Created.year == dataset.Closed_Won_Date.year \
            and (dataset.My_Date_Created.month == dataset.Closed_Won_Date.month):
        return 1
    else:
        return 0


def create_closed_won_data(dataset):
    dataset['ID_Type'] = dataset.apply(id_type_fun, axis=1)
    # print(np.unique(dataset.ID_Type, return_counts=True))

    dataset['Generic_ID'] = dataset.apply(generic_id_fun, axis=1)
    # print(dataset.Generic_ID.unique())

    dataset['New_Lead_Status'] = dataset.apply(new_lead_status_fun, axis=1)
    # print(dataset.New_Lead_Status.unique())

    dataset['Bool_Ignore_Rows'] = dataset.apply(ignore_rows_fun, axis=1)
    # print(np.unique(dataset.Bool_Ignore_Rows, return_counts=True))

    dataset['My_Date_Created'] = dataset.apply(my_date_created_fun, axis=1)
    dataset['My_Date_Created'] = pd.to_datetime(dataset['My_Date_Created'], format='%Y-%m-%d')
    # print(dataset.My_Date_Created.unique())

    dataset['My_Subscription_Plan'] = dataset.apply(my_subscription_plan_fun, axis=1)
    # print(dataset.My_Subscription_Plan.unique())

    dataset['Bool_Demo_Booked'] = dataset.apply(demo_booked_fun, axis=1)
    # print(np.unique(dataset.Bool_Demo_Booked, return_counts=True))

    dataset['Bool_Closed_Won'] = dataset.apply(closed_won_fun, axis=1)
    # print(np.unique(dataset.Bool_Closed_Won, return_counts=True))

    dataset['Closed_Won_Date'] = dataset.apply(closed_won_date_fun, axis=1)
    dataset['Closed_Won_Date'] = pd.to_datetime(dataset['Closed_Won_Date'], format='%Y-%m-%d')
    # print(dataset.Closed_Won_Date.unique())

    dataset['Bool_Closed_Won_Same_Month'] = dataset.apply(closed_won_same_month_fun, axis=1)
    # print(np.unique(dataset.Bool_Closed_Won_Same_Month, return_counts=True))

    dataset = dataset[dataset.Bool_Ignore_Rows == 0]
    dataset = dataset[["My_Date_Created", "Generic_ID", "Closed_Won_Date", "Bool_Closed_Won_Same_Month"]]

    return dataset


def create_aggregated_data(dataset):

    # dataframe with dates from 1/1/2021 to today
    starting_date = datetime.strptime("2021-01-01", '%Y-%m-%d').date()
    ending_date = date.today()
    new_date_range = pd.date_range(starting_date, ending_date)

    closed_won_df = dataset.groupby(by=["My_Date_Created"]).sum()
    closed_won_df.columns = ["Closed_Won_M0"]
    closed_won_df = closed_won_df[closed_won_df.index >= "2021-01-01"]

    leads_df = dataset[['My_Date_Created', 'Generic_ID']].groupby(by=["My_Date_Created"]).nunique()
    leads_df.columns = ["Daily_Signups"]
    leads_df = leads_df[leads_df.index >= "2021-01-01"]

    daily_df = pd.concat([closed_won_df, leads_df], axis=1)
    daily_df = daily_df.reindex(new_date_range, fill_value=0)

    cumul_df = daily_df.groupby([daily_df.index.year, daily_df.index.month]).cumsum()
    cumul_df.columns = ["Cumulative_Closed_Won_M0", "Cumulative_Signups"]

    my_daily_df = pd.concat([daily_df, cumul_df], axis=1)

    my_daily_df["Daily_CR_Closed_Won_M0"] = np.round((my_daily_df.Closed_Won_M0 / my_daily_df.Daily_Signups) * 100, 2)
    my_daily_df["Cumulative_CR_Closed_Won_M0"] = np.round(
                                                (my_daily_df.Cumulative_Closed_Won_M0 / my_daily_df.Cumulative_Signups)
                                                * 100, 2)

    my_daily_df["Daily_CR_Closed_Won_M0"] = my_daily_df["Daily_CR_Closed_Won_M0"].fillna(0)
    my_daily_df["Cumulative_CR_Closed_Won_M0"] = my_daily_df["Cumulative_CR_Closed_Won_M0"].fillna(0)

    return my_daily_df


def get_summarised_data(data, predictions):

    dataset = pd.concat([data, predictions], axis=1)
    dataset = dataset.reset_index()
    dataset.columns = ["Date",
                       "Closed_Won_M0", "Daily_Signups",
                       "Cumulative_Closed_Won_M0", "Cumulative_Signups",
                       "Daily_CR_Closed_Won_M0",
                       "Cumulative_CR_Closed_Won_M0", "ARIMA_predictions"]

    dataset.Daily_CR_Closed_Won_M0 = dataset.Daily_CR_Closed_Won_M0 / 100
    dataset.Cumulative_CR_Closed_Won_M0 = dataset.Cumulative_CR_Closed_Won_M0 / 100
    dataset.ARIMA_predictions = dataset.ARIMA_predictions / 100

    dataset.CR_M0_actuals = np.round(dataset.Cumulative_CR_Closed_Won_M0, 5)
    dataset.ARIMA_predictions = np.round(dataset.ARIMA_predictions, 5)

    return dataset
