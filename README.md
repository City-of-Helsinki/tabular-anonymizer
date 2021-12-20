# Tabular Anonymizer

Anonymization and pseudonymization tools for tabular data. Make data k-anonymous with Mondrian-algorithm.

## Pseudonymization

Pseudonymization tool is intended for combining data from multiple sources. The user can pseudonymize a direct identifier with a hash function.

    # Example of pseudonymization
    from tabular_anonymizer import utils
    
    # Simple way
    utils.pseudonymize(df, 'column_name', generate_nonce=True)

    # Pseudonymize two datasets
    nonce1 = utils.create_nonce()
    nonce2 = utils.create_nonce()
    utils.pseudonymize(df1, 'column_name1', nonce1, nonce2)
    utils.pseudonymize(df2, 'column_name2', nonce1, nonce2)

    # combine (merge) two datasets with common index column and pseudonymize
    df_c = combine_and_pseudonymize(df1, df2, 'id')        

## Anonymization

### K-Anonymity with MondrianAnonymizer

    # Example code

    import pandas as pd
    from tabular_anonymizer import DataFrameAnonymizer

    # Setup dataframe
    df = pd.read_csv("./adult.csv", sep=",")
    
    # Define sensitive attributes
    sensitive_columns = ['label']

    # Anonymize dataframe with k=10
    p = MondrianAnonymizer(df, sensitive_columns)
    df_anonymized = p.anonymize_k_anonymity(k=10)

### Post-processing

#### Partial masking

    # Convert intervals to partially masked ['20220', '20210'] => '202**'
    generalize(df, 'zip', generalize_partial_masking)


    # Original table
    #                               id|    zip
    #                               1 | '20220'
    #                               2 | '20210'
    # Anonymized table (K=2)
    #                               zip
    #                               ['20220', '20210']
    # After partial masking
    #                               zip
    #                               '202**'




### More examples

    More examples can be found in examples folder.

### Docker and Jupyterlab

You can run Jupypterlab and do experiments with tabular anonymizer in docker container:

     docker build . -t tabular-anonymizer && docker run --rm -it -p 8888:8888 tabular-anonymizer  

Hit ctrl + c to quit container.

## Acknowledgements

This library is an enhanced version of AnonyPy mondrian implementation. 

See https://github.com/glassonion1/anonypy

