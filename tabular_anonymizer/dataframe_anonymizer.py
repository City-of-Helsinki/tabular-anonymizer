from typing import List

import pandas as pd
from pandas import DataFrame
from pandas.core.dtypes.common import is_numeric_dtype
from pandas.core.indexes.numeric import NumericIndex

from .mondrian_anonymizer import MondrianAnonymizer


class DataFrameAnonymizer:
    AVG_OVERWRITE = True
    mondrian: MondrianAnonymizer    # takes care of partitioning dataframe using mondrian algorithm

    def __init__(self, sensitive_attribute_columns: List[str], feature_columns=None, avg_columns=None, format_to_str=False):
        self.sensitive_attribute_columns = sensitive_attribute_columns
        self.feature_columns = feature_columns
        self.avg_columns = avg_columns
        self.format_to_str = format_to_str

    # Set feature colums from all other columns than sensitive columns
    def init_feature_colums(self, df):
        # Setup feature columns / Quasi identifiers
        fc = []
        if self.feature_columns is None:
            # Assume that all other columns are feature columns
            for col in df.columns:
                if col not in self.sensitive_attribute_columns:
                    fc.append(col)
            self.feature_columns = fc

    def anonymize(self, df, k, l=0):

        # Check inputs
        if df is None or len(df) == 0:
            raise Exception("Dataframe is empty")
        if self.sensitive_attribute_columns is None or len(self.sensitive_attribute_columns) == 0:
            raise Exception("Provide at least one sensitive attribute column")

        if not self.feature_columns:
            self.init_feature_colums(df)

        if self.avg_columns:
            for c in self.avg_columns:
                if not is_numeric_dtype(df[c]):
                    raise Exception("Column " + c + " is not numeric and average cannot be calculated.")

        mondrian = MondrianAnonymizer(df, self.feature_columns, self.sensitive_attribute_columns)
        partitions = mondrian.partition(k, l)
        dfa = self.build_anonymized_dataframe(df, partitions)
        return dfa

    def anonymize_k_anonymity(self, df, k) -> DataFrame:
        return self.anonymize(df, k)

    def anonymize_l_diversity(self, df, k, l) -> DataFrame:
        return self.anonymize(df, k, l=l)

    def anonymize_t_closeness(self, df, k) -> DataFrame:
        return self.anonymize(df, k)

    @staticmethod
    def __agg_column_str(series):
        if is_numeric_dtype(series):
            minimum = series.min()
            maximum = series.max()
            return "{min} - {max}".format(min=minimum, max=maximum)
        else:
            series.astype("category")
            l = [str(n) for n in set(series)]
            return ", ".join(l)

    @staticmethod
    def __agg_column_list(series):
        if is_numeric_dtype(series):
            minimum = series.min()
            maximum = series.max()
            return [minimum, maximum]
        else:
            series.astype("category")
            l = [str(n) for n in set(series)]
            return l

    def partition_dataframe(self, df, k, l=0) -> List[NumericIndex]:
        mondrian = MondrianAnonymizer(df, self.feature_columns, self.sensitive_attribute_columns)
        partitions = mondrian.partition(k, l)
        return partitions

    def build_anonymized_dataframe(self, df, partitions) -> DataFrame:
        aggregations = {}
        sensitive_columns = self.sensitive_attribute_columns
        feature_columns = self.feature_columns


        sa_len = len(sensitive_columns)
        for column in feature_columns:
            if self.format_to_str:
                aggregations[column] = self.__agg_column_str
            else:
                aggregations[column] = self.__agg_column_list

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
