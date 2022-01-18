import os

import pandas as pd

from tabular_anonymizer import utils


def main():
    # Prepare dataframe
    this_dir, this_filename = os.path.split(__file__)
    file1 = os.path.join(this_dir, "./adult.csv")
    print("Read file to dataframe", file1)
    df = pd.read_csv(file1, sep=",", index_col=0)
    df.reset_index()
    df.index = range(len(df))

    # Declare column you want to pseudonymize
    pseudonymized_col = 'education'

    # Print sample of colum values to be pseudonymized
    print(df.head()[pseudonymized_col])

    # Pseudonymize selected column
    utils.pseudonymize(df, pseudonymized_col, generate_nonce=True)

    print("\nReady:")
    print(df.head()[pseudonymized_col])


main()
