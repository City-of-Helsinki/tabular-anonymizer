# Tabular Anonymizer

Anonymization and pseudonymization tools for tabular data. Make data k-anonymous with Mondrian-algorithm.

## Pseudonymization

Pseudonymization tool is intended for combining data from multiple sources. The user can pseudonymize a direct identifier with a hash function.

## How to use

### K-Anonymity with MondrianAnonymizer

    import pandas as pd
    from tabular_anonymizer import DataFrameAnonymizer

    # Setup dataframe
    df = pd.read_csv("./adult.csv", sep=",")
    
    # Define sensitive attributes
    sensitive_columns = ['label']

    # Anonymize dataframe with k=10
    p = MondrianAnonymizer(df, sensitive_columns)
    df_anonymized = p.anonymize_k_anonymity(k=10)

### More examples

    See examples - folder for mo

### Docker and Jupyterlab

Run jupyterlab in container:docker

     docker build . -t tabular-anonymizer && docker run --rm -it -p 8888:8888 tabular-anonymizer  

## Acknowledgements

    This library is an enhanced version of AnonyPy mondrian implementation. See https://github.com/glassonion1/anonypy

