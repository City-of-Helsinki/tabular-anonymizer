import pandas as pd
from tabular_anonymizer import DataFrameAnonymizer, utils


def log(message):
    print("\n")
    print(message)
    print("------------------------------")


def main():
    # Prepare dataframe
    log("Prepare dataframes")
    file1 = "./adult.csv"
    print("Read file ", file1)
    df = pd.read_csv(file1, sep=",", index_col=0)
    utils.prepare_index_for_anonymization(df)
    print(df.head())

    # Setup Sensitive attributes you dont want to alter
    sensitive_columns = ['label']
    # Avg columns
    avg_columns = ['capital-gain', 'capital-loss']

    # Run tabular_anonymizer
    # Set k
    k = 10
    log("Run tabular_anonymizer")
    print("Run tabular_anonymizer. Sensitive columns: ", sensitive_columns, ", k=", k)
    p = DataFrameAnonymizer(df, sensitive_columns, avg_columns=avg_columns)

    # New anonymized dataframe is formed
    df_anonymized = p.anonymize_k_anonymity(k=k)

    # Save anonymized dataframe to file
    # outfile = './anonymized-adult.csv'
    # print("Write dataframe to ", outfile)
    # df_anonymized.to_csv(path_or_buf=outfile)
    print("Ready")
    print(df_anonymized.head())


main()
