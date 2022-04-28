from datetime import timedelta

from statsmodels.tsa.stattools import adfuller
from enodo import EnodoModule


class BaseModule:
    def __init__(self, dataset, params, series_name, siridb_client):
        """
        Start modelling a time serie
        :param series_name: name of the serie
        :param dataset: dataframe (Panda) with datapoints
        """
        raise NotImplementedError

    @classmethod
    def get_module_info(cls) -> EnodoModule:
        raise NotImplementedError


def adf_stationarity_test(timeseries):

    # Dickey-Fuller test:
    adf_test = adfuller(timeseries[1], autolag='AIC')

    p_value = adf_test[1]

    return p_value >= .05


def find_frequency(datetime_list):

    summed_freq = timedelta(seconds=0)
    summed_count = 0
    i = 1
    while i < len(datetime_list):
        if (i - 1) in datetime_list and i in datetime_list:
            summed_freq += abs(datetime_list[i - 1] - datetime_list[i])
            summed_count += 1
        i += 1

    return summed_freq / (summed_count)


# -----------------------------------------------------------------------------
# accept a dataframe, remove outliers, return cleaned data in a new dataframe
# see http://www.itl.nist.gov/div898/handbook/prc/section1/prc16.htm
# -----------------------------------------------------------------------------
def remove_outlier_in_df(df_in, col_name):
    q1 = df_in[col_name].quantile(0.05)
    q3 = df_in[col_name].quantile(0.95)
    iqr = q3 - q1  # Interquartile range
    fence_low = q1 - 1.5 * iqr
    fence_high = q3 + 1.5 * iqr
    df_out = df_in.loc[
        (df_in[col_name] > fence_low) & (df_in[col_name] < fence_high)]
    return df_out
