import pandas as pd

from tabular_anonymizer import utils


def log(message):
    print("\n")
    print(message)
    print("------------------------------")


def main():
    # Column you want to pseudonymize
    pseudonymized_col = 'education'

    # Prepare dataframe
    log("Prepare dataframes")
    file1 = "./adult.csv"
    print("Read file ", file1)
    df = pd.read_csv(file1, sep=",", index_col=0)
    df.reset_index()
    df.index = range(len(df))
    print(df.head()[pseudonymized_col])

    # Pseudonymize
    utils.pseudonymize(df, pseudonymized_col, generate_nonce=True)

    print("Ready")
    print(df.head()[pseudonymized_col])


main()
