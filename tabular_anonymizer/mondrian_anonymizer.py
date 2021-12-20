from anonypy import anonymity


"""
Modified and optimized version of anonypy Mondrian that supports multiple sensitive attributes. 
"""


class MondrianAnonymizer:
    feature_columns = []  # Columns that are used in anonymization process. Data will be grouped using these columns.
    sensitive_columns = []  # This data will not get altered
    max_split_count = 1000000  # failsafe that prevents algorithm to get stuck to infinite loop
    split_count = 0
    avg_columns = []  # Numeric columns that are converted to average value after partitioning (instead of interval)
    df = None    # Original dataframe

    def __init__(self, df, feature_columns, sensitive_columns):
        self.df = df
        if not feature_columns:
            raise Exception("Feature columns is mandatory parameter")
        self.sensitive_columns = sensitive_columns
        self.feature_columns = feature_columns
        self.split_count = 0

    def is_valid(self, partition, k=2, l=0, p=0.0):
        # k-anonymous
        if not anonymity.is_k_anonymous(partition, k):
            return False
        # l-diverse
        if l > 0 and self.sensitive_columns is not None:
            for sensitive_column in self.sensitive_columns:
                diverse = anonymity.is_l_diverse(
                    self.df, partition, sensitive_column, l
                )
                if not diverse:
                    return False
        # t-close
        if p > 0.0 and self.sensitive_columns is not None:
            for sensitive_column in self.sensitive_columns:
                global_freqs = anonymity.get_global_freq(self.df, sensitive_column)
                close = anonymity.is_t_close(
                    self.df, partition, sensitive_column, global_freqs, p
                )
                if not close:
                    return False
        return True

    def get_spans(self, partition, scale=None):
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

    def split(self, column, partition):
        self.split_count += 1
        if self.split_count > self.max_split_count:
            raise Exception(
                "Abort: Maximum amount of split operations exceeded: {max}. Check your dataset and parameters.".format(
                    max=self.max_split_count))
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

    def partition(self, k=3, l=0, p=0.0):
        scale = self.get_spans(self.df.index)
        finished_partitions = []
        partitions = [self.df.index]
        while partitions:
            partition = partitions.pop(0)
            spans = self.get_spans(partition, scale)
            for column, span in sorted(spans.items(), key=lambda x: -x[1]):
                lp, rp = self.split(column, partition)
                if not self.is_valid(lp, k, l, p) or not self.is_valid(rp, k, l, p):
                    continue
                partitions.extend((lp, rp))
                break
            else:
                finished_partitions.append(partition)
        return finished_partitions
