import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.graphics.tsaplots import plot_acf
from statsmodels.graphics.tsaplots import plot_pacf
from statsmodels.tsa.arima.model import ARIMA
from sklearn.metrics import mean_squared_error
from sklearn.metrics import r2_score

import warnings
warnings.filterwarnings("ignore")


def data_pre_processing(dataset):

    # Fill null values with zero
    dataset = dataset.fillna(0)
    dataset = dataset.dropna()

    # Reset index of the dataset
    dataset = dataset.reset_index(drop=True)

    # Change data types of columns to datetime and float for dates and cost
    dataset.Date = pd.to_datetime(dataset.Date)
    dataset.index = dataset.Date
    dataset = dataset.drop(["Date"], axis=1)

    return dataset


def find_arima_parameters(data, show_plots=True):

    if show_plots:
        plt.rcParams.update({'figure.figsize': (9, 7), 'figure.dpi': 120})

        # Original Series
        fig, (ax1, ax2, ax3) = plt.subplots(3)

        ax1.plot(data)
        ax1.set_title('Original Series')
        ax1.axes.xaxis.set_visible(False)

        # 1st Differencing
        ax2.plot(data.CR_M0.diff())
        ax2.set_title('1st Order Differencing')
        ax2.axes.xaxis.set_visible(False)

        # 2nd Differencing
        ax3.plot(data.diff().diff())
        ax3.set_title('2nd Order Differencing')
        plt.show()

        fig, (ax1, ax2, ax3) = plt.subplots(3)

        plot_acf(data, ax=ax1)
        plot_acf(data.diff().dropna(), ax=ax2)
        plot_acf(data.diff().diff().dropna(), ax=ax3)

        plot_pacf(data)
        plot_acf(data)


def performance_metrics(dataset, predicted):

    MSE_metric = np.round(mean_squared_error(dataset, predicted), 2)
    r2_score_metric = r2_score(dataset, predicted) * 100

    # print("MSE: {}".format(np.round(MSE_metric, 2)))
    # print("R2 Score: {}".format(np.round(r2_score_metric), 2))

    metrics_list = [MSE_metric, r2_score_metric]

    return metrics_list


def apply_ARIMA(data, parameters_set):

    p, d, q = parameters_set
    # ======================

    # Train ARIMA model
    arima_model = ARIMA(data, order=(p, d, q))
    model = arima_model.fit()

    preds_actuals = model.get_prediction()
    predictions = preds_actuals.predicted_mean

    preds_14_days = model.get_forecast(14)
    predictions_14_days = preds_14_days.predicted_mean

    predictions_df = pd.DataFrame(pd.concat([predictions, predictions_14_days]))
    predictions_df.values[predictions_df < 0] = 0

    conf_int_actuals = preds_actuals.conf_int(alpha=0.10)
    conf_int_14_days = preds_14_days.conf_int(alpha=0.10)

    lower_conf_int_14_days = conf_int_14_days['lower CR_M0']
    upper_conf_int_14_days = conf_int_14_days['upper CR_M0']

    lower_conf_int_14_days.values[lower_conf_int_14_days < 0] = 0
    upper_conf_int_14_days.values[upper_conf_int_14_days < 0] = 0

    pred_conf_int = [lower_conf_int_14_days, upper_conf_int_14_days]

    return predictions_df, pred_conf_int, parameters_set


def run_ARIMA(data, params_set, var_show_plots=False):

    data = data[["Cumulative_CR_Closed_Won_M0"]]
    data = data.reset_index()
    data.columns = ["Date", "CR_M0"]

    data = data_pre_processing(data)

    find_arima_parameters(data, var_show_plots)

    # params_set = [3, 0, 5]

    predictions_df, pred_conf_int, params_set = apply_ARIMA(data, params_set)

    list_metrics = performance_metrics(data, predictions_df[:-14])

    return data, predictions_df, pred_conf_int, list_metrics, params_set


def plot_arima_predictions(ARIMA_data, ARIMA_predictions):

    plt.plot(ARIMA_data.index,          ARIMA_data.CR_M0,     label="Actuals",                  color="red")
    plt.plot(ARIMA_predictions.index,   ARIMA_predictions,    label="ARIMA(3, 0, 5)",           color="green")

    plt.scatter(ARIMA_data.index,          ARIMA_data.CR_M0,    color='red',       s=10)
    plt.scatter(ARIMA_predictions.index,   ARIMA_predictions,   color="green",     s=10)

    plt.axvline(x=ARIMA_predictions.index[-15], color='k', linestyle='--')

    plt.title("Actuals vs Predicted - Conversion Rate M0 (%)")
    plt.xlabel('Date')
    plt.ylabel('Conversion Rate M0 (%)')
    plt.legend()
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.legend(ncol=6, loc=9)
    plt.plot()
