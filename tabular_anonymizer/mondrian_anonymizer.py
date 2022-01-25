from typing import List
from pandas.api.types import is_numeric_dtype
from pandas import Int64Index, DataFrame

"""
Modified and optimized version of anonypy Mondrian that supports multiple sensitive attributes. 
"""


class MondrianAnonymizer:
    feature_columns: List[str] = []     # Quasi-identifiers. Data will be grouped using these columns.
    sensitive_columns: List[str] = []  # This data will not get altered
    max_split_count: int = 1000000  # failsafe that prevents algorithm to get stuck to infinite loop
    split_count: int = 0
    avg_columns: List[str] = []  # Numeric columns that are converted to average value after partitioning
    df: DataFrame = None    # Original dataframe
    _DEFAULT_K: int = 3

    def __init__(self, df: DataFrame, feature_columns: List[str], sensitive_columns: List[str]) -> None:
        # prepare dataframe for partitioning
        self.df = self.prepare_dataframe(df)
        if not feature_columns:
            raise Exception("Feature columns is mandatory parameter")
        self.sensitive_columns = sensitive_columns
        self.feature_columns = feature_columns
        self.split_count = 0

    def prepare_dataframe(self, df_orig: DataFrame):
        df = df_orig.__deepcopy__()

        # Set non-numerical columns to category
        for col in df.columns:
            if not is_numeric_dtype(df[col]):
                df[col] = df[col].astype("category")

        return df

    def is_valid(self, partition: Int64Index, k: int = _DEFAULT_K, l: int = 0) -> bool:
        # k-anonymous
        if not self.is_k_anonymous(partition, k):
            return False
        # l-diverse
        if l > 0 and self.sensitive_columns is not None:
            for sensitive_column in self.sensitive_columns:
                diverse = self.is_l_diverse(
                    self.df, partition, sensitive_column, l
                )
                if not diverse:
                    return False
        return True

    def is_k_anonymous(self, partition, k):
        if len(partition) < k:
            return False
        return True

    def is_l_diverse(self, df, partition, sensitive_column, l):
        diversity = len(df.loc[partition][sensitive_column].unique())
        return diversity >= l

    def get_spans(self, partition: Int64Index, scale: dict = None) -> dict:
        spans = {}
        # get subset of original dataframe representing this partition and use it for faster calculations
        dfp = self.df.loc[partition]
        for column in self.feature_columns:
            if self.df[column].dtype.name == "category":
                span = len(dfp[column].unique())
            else:
                span = dfp[column].max() - dfp[column].min()
            if scale is not None and column in scale and scale[column] != 0:
                span = span / scale[column]
            spans[column] = span
        return spans

    def split(self, column: str, partition: Int64Index) -> (Int64Index, Int64Index):
        self.split_count += 1
        if self.split_count > self.max_split_count:
            raise Exception(
                "Abort: Maximum amount of split operations exceeded: {max}. "
                "Check your dataset and parameters.".format(max=self.max_split_count))
        dfp = self.df[column][partition]
        if dfp.dtype.name == "category":
            values = dfp.unique()
            lv = set(values[: len(values) // 2])
            rv = set(values[len(values) // 2:])
            return dfp.index[dfp.isin(lv)], dfp.index[dfp.isin(rv)]
        else:
            median = dfp.median()
            dfl = dfp.index[dfp < median]
            dfr = dfp.index[dfp >= median]
            return dfl, dfr

    def partition(self, k: int = _DEFAULT_K, l: int = 0) -> object:
        scale = self.get_spans(self.df.index)
        finished_partitions = []
        partitions = [self.df.index]
        while partitions:
            partition = partitions.pop(0)
            spans = self.get_spans(partition, scale)
            sorted_items = sorted(spans.items(), key=lambda x: -x[1])
            for column, span in sorted_items:
                lp, rp = self.split(column, partition)
                if not self.is_valid(lp, k, l) or not self.is_valid(rp, k, l):
                    continue
                partitions.extend((lp, rp))
                break
            else:
                finished_partitions.append(partition)
        return finished_partitions
