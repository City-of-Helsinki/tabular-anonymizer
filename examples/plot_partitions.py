import pandas as pd
from tabular_anonymizer import DataFrameAnonymizer, utils
from mondrian_plot import plot


def main():

    #
    # Visualization of mondrian algorithm
    #

    # Prepare dataframe
    file1 = "./mock_salary_data.csv"
    df = pd.read_csv(file1, sep=",", index_col=0)
    utils.prepare_index_for_anonymization(df)
    print("\nDataset:\n")
    print(df)

    # Define parameters
    # Setup Sensitive attributes you don't want to alter
    sensitive_columns = ['education']

    # Declare K-value
    k = 5

    # Init DataFrameAnonymizer with dataframe and sensitive column
    p = DataFrameAnonymizer(sensitive_columns, feature_columns=['age', 'salary'])

    # Perform partiotioning and anonymization separately
    print("\nPartition dataframe with mondrian algorithm. Sensitive columns: ", sensitive_columns, ", k=", k)
    partitions = p.partition_dataframe(df, k=k)

    # Data in anonymized form
    df_anonymized = p.build_anonymized_dataframe(df, partitions)

    # Plot partition
    # Use age and salary as dimensions since education is sensitive column
    plot(df, partitions, sensitive_columns, column_x='age', column_y='salary')

    # Print anonymization results for comparison.
    print("\nAnonymized data, K=", k)
    print(df_anonymized)

main()
