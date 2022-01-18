import os

import pandas as pd
from tabular_anonymizer import DataFrameAnonymizer, utils

def main():

    # Prepare dataframe from example csv file
    this_dir, this_filename = os.path.split(__file__)
    file1 = os.path.join(this_dir, "./adult.csv")
    print("Read file to dataframe", file1)
    df = pd.read_csv(file1, sep=",", index_col=0)
    utils.prepare_index_for_anonymization(df)

    # Use subset of data for demonstration purposes
    df = df.head(1000)

    # Setup Sensitive attributes you don't want to alter
    sensitive_columns = ['label']

    # Avg columns
    # This calculates mean values of combined rows instead of interval-values after anonymization
    avg_columns = ['capital-gain', 'capital-loss']

    # Set k
    k = 10

    # Init DataFrameAnonymizer
    print("Run tabular_anonymizer. Sensitive columns: ", sensitive_columns, ", k=", k)
    p = DataFrameAnonymizer(df, sensitive_columns, avg_columns=avg_columns, format_to_str=True)

    # New anonymized dataframe is formed
    df_anonymized = p.anonymize_k_anonymity(k=k)

    print("\nSample of anonymized data:\n")
    print(df_anonymized.head())


main()
