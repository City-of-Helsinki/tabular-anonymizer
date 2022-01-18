import pandas as pd
from pandas import DataFrame
from pandas.api.types import is_numeric_dtype

from .mondrian_anonymizer import MondrianAnonymizer


class DataFrameAnonymizer:
    AVG_OVERWRITE = True
    mondrian: MondrianAnonymizer    # takes care of partitioning dataframe using mondrian algorithm

    def __init__(self, df, sensitive_attribute_columns, feature_columns=None, avg_columns=None):
        if df is None or len(df) == 0:
            raise Exception("Dataframe is empty")
        if feature_columns is None:
            # Assume that all other columns are feature columns
            feature_columns = []
            for col in df.columns:
                if col not in sensitive_attribute_columns:
                    feature_columns.append(col)
        self.set_non_numerical_as_categorical(df)
        self.mondrian = MondrianAnonymizer(df, feature_columns, sensitive_attribute_columns)

        self.avg_columns = avg_columns
        self.interval_columns = []
        if avg_columns:
            for c in avg_columns:
                if not is_numeric_dtype(df[c]):
                    raise Exception("Column " + c + " is not numeric and cannot be used as avg_column.")
        for c in feature_columns:
            if is_numeric_dtype(df[c]) and not c in avg_columns:
                self.interval_columns.append(c)

    def __anonymize(self, k, l=0, t=0.0):
        partitions = self.mondrian.partition(k, l, t)
        return self.build_anonymized_dataframe(partitions)

    def anonymize_k_anonymity(self, k):
        return self.__anonymize(k)

    def anonymize_l_diversity(self, k, l):
        return self.__anonymize(k, l=l)

    def anonymize_t_closeness(self, k, t):
        return self.__anonymize(k, t=t)

    def mark_column_as_zipcode(self, column_name):
        self.mondrian.df[column_name] = self.mondrian.df[column_name].astype(str).str.zfill(5)
        self.mondrian.df[column_name] = self.mondrian.df[column_name].astype("category")

    def mark_datetime_to_year(self, column_name):
        self.mondrian.df[column_name] = pd.to_datetime( self.mondrian.df[column_name])
        self.mondrian.df[column_name] = pd.DatetimeIndex(self.mondrian.df[column_name]).year

    # Handling for zip codes and other numeric categorical columns
    def mark_as_categorical(self, categorical_columns):
        for name in categorical_columns:
            self.mondrian.df[name] = self.mondrian.df[name].astype("category")

    @staticmethod
    def set_non_numerical_as_categorical(df):
        for col in df.columns:
            if not is_numeric_dtype(df[col]):
                df[col] = df[col].astype("category")


    @staticmethod
    def __agg_categorical_column(series):
        # this is workaround for dtype bug of series
        series.astype("category")
        l = [str(n) for n in set(series)]
        # return [",".join(l)]
        # return list instead of string
        return l

    @staticmethod
    def __agg_numerical_column(series):
        minimum = series.min()
        maximum = series.max()
        if maximum == minimum:
            return [maximum]
        return [minimum, maximum]

    def partition_dataframe(self, k, l=0, t=0.0):
        partitions = self.mondrian.partition(k, l, t)
        return partitions

    def build_anonymized_dataframe(self, partitions) -> DataFrame:
        aggregations = {}
        sensitive_columns = self.mondrian.sensitive_columns
        feature_columns = self.mondrian.feature_columns
        df = self.mondrian.df

        sa_len = len(sensitive_columns)
        for column in feature_columns:
            if df[column].dtype.name == "category":
                aggregations[column] = self.__agg_categorical_column
            else:
                aggregations[column] = self.__agg_numerical_column
        rows = []
        for i, partition in enumerate(partitions):
            dfp = df.loc[partition]
            grouped_columns = dfp.agg(aggregations, squeeze=False)
            values = grouped_columns.to_dict()
            if self.avg_columns:
                # handle average columns and set average instead of interval
                # overwrite column with average
                for avg_col in self.avg_columns:
                    col_name = avg_col + '_avg' if not self.AVG_OVERWRITE else avg_col
                    if avg_col in feature_columns:
                        avg_val = dfp[avg_col].mean()
                        values.update({col_name: avg_val})
            if self.interval_columns:
                # set interval high and low values to separate columns
                for interval_col in self.interval_columns:
                    col_name_lo = interval_col + '_lo'
                    col_name_hi = interval_col + '_hi'
                    value = values[interval_col]
                    value_low = value[0]
                    if len(value) > 1:
                        value_high = value[1]
                    else:
                        value_high = value_low
                    values.update({col_name_lo: value_low, col_name_hi: value_high})
            grouped_sensitive_columns = dfp.groupby(sensitive_columns, as_index=False)
            for grouped_sensitive_value in grouped_sensitive_columns:
                for sensitive_column in sensitive_columns:
                    if sa_len > 1:
                        # Value is tuple
                        sensitive_value = grouped_sensitive_value[0][sensitive_columns.index(sensitive_column)]
                    else:
                        sensitive_value = grouped_sensitive_value[0]
                    count = len(grouped_sensitive_value[1])
                    values.update(
                        {
                            sensitive_column: sensitive_value,
                            sensitive_column + "_count": count,
                        }
                    )
                rows.append(values.copy())
        return pd.DataFrame(rows)
