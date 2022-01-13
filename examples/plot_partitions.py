import pandas as pd
from tabular_anonymizer import DataFrameAnonymizer, utils
import matplotlib.pylab as pl
import matplotlib.patches as patches
import matplotlib.colors as mcolors


def __build_indexes(df):
    indexes = {}
    for column in df:
        if df[column].dtype.name == "category":
            values = sorted(df[column].unique())
            indexes[column] = {x: y for x, y in zip(values, range(len(values)))}
    return indexes


def __get_bounds(df, column, indexes, offset=1.0):
    dfp = df[column]
    if dfp.dtype.name == "category":
        return 0-offset, len(indexes[column])+offset
    return df[column].min()-offset, df[column].max()+offset


def __get_coords(df, column, partition, indexes, offset=0.1):
    if df[column].dtype.name == "category":
        sv = df[column][partition].sort_values()
        first = sv[sv.index[0]]
        last = sv[sv.index[-1]]
        l, r = indexes[column][first], indexes[column][last]
        print(column, l, r)
    else:
        sv = df[column][partition].sort_values()
        next_value = sv[sv.index[-1]]
        larger_values = df[df[column] > next_value][column]
        if len(larger_values) > 0:
            next_value = larger_values.min()
        l = sv[sv.index[0]]
        r = next_value
    l -= offset
    r += offset
    return l, r


def __plot_rects(df, ax, rects, column_x, column_y, edgecolor='black', facecolor='none'):
    indexes = __build_indexes(df)
    colors = list(mcolors.XKCD_COLORS.values())
    c = 0
    for (xl, yl), (xr, yr) in rects:
        facecolor = colors[c]
        ax.add_patch(patches.Rectangle((xl, yl), xr-xl, yr-yl, linewidth=1, edgecolor=edgecolor, facecolor=facecolor, alpha=0.5))
        c += 1
        if c >= len(colors):
            c = 0

    ax.set_xlim(*__get_bounds(df, column_x, indexes))
    ax.set_ylim(*__get_bounds(df, column_y, indexes))
    ax.set_xlabel(column_x)
    ax.set_ylabel(column_y)


def __get_partition_rects(df, partitions, column_x, column_y, indexes, offsets=[0.1, 0.1]):
    rects = []
    for partition in partitions:
        xl, xr = __get_coords(df, column_x, partition, indexes, offset=offsets[0])
        yl, yr = __get_coords(df, column_y, partition, indexes, offset=offsets[1])
        rects.append(((xl, yl), (xr, yr)))
    return rects


def plot(df_orig, partitions, sensitive_columns, column_x=None, column_y=None):
    indexes = __build_indexes(df_orig)
    df = df_orig.replace(indexes)
    all_columns = df.columns
    feature_columns = list(set(all_columns) - set(sensitive_columns))
    if not column_x and not column_y:
        column_x, column_y = feature_columns[:2]
    rects = __get_partition_rects(df, partitions, column_x, column_y, indexes, offsets=[0.0, 0.0])
    pl.figure(figsize=(10, 10))
    ax = pl.subplot(111)
    __plot_rects(df, ax, rects, column_x, column_y, facecolor='r')
    for partition in partitions:
        pl.scatter(df.loc[partition][column_x], df.loc[partition][column_y], 200)
    for index, row in df_orig.iterrows():
        pl.annotate(row[sensitive_columns[0]], (row[column_x], row[column_y]))
    pl.show()


def main():
    # Prepare dataframe
    file1 = "./mock_salary_data.csv"
    print("Read file ", file1)
    df = pd.read_csv(file1, sep=",", index_col=0)
    utils.prepare_index_for_anonymization(df)
    print(df)

    # Setup Sensitive attributes you dont want to alter
    sensitive_columns = ['education']

    # Set k and init anonymizer
    k = 5
    print("Run tabular_anonymizer. Sensitive columns: ", sensitive_columns, ", k=", k)
    p = DataFrameAnonymizer(df, sensitive_columns)

    # Partition dataframe
    partitions = p.partition_dataframe(k=k)
    df_anonymized = p.build_anonymized_dataframe(partitions)

    # Plot partition and print anonymized table
    plot(df, partitions, sensitive_columns, column_x='age', column_y='salary')
    print(df_anonymized)


main()
