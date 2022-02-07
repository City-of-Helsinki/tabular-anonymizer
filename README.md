# Tabular Anonymizer

Anonymization and pseudonymization tools for tabular data. 


This library provides tools and methods for anonymizing tabular data and privacy protection.

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) for installation.

        $ pip install https://github.com/Datahel/tabular-anonymizer.git

You can also clone this repository and install library from local folder with pip using -e flag:

    $ git clone https://github.com/Datahel/tabular-anonymizer.git
    $ pip install -e tabular-anonymizer


## Usage

### Anonymization

DataFrameAnonymizer anonymization functionality supports K-anonymity alone or together with L-diversity or T-closeness 
using Mondrian algorithm. Utils and methods are for [Pandas DataFrames](https://pandas.pydata.org). 

#### K-Anonymity in practice

In this simplified example there is mock dataset of 20 persons about their age, salary and education. 
We will anonymize this using mondrian algorithm with K=5.

![Dataframe before anonymization](documents/mondrian_data.png?raw=true "Dataframe")

After mondrian partitioning process (with K=5), data is divided to groups of at least 5 using age and salary as dimensions.

![Values after mondrian partitioning](documents/mondrian_plot.png?raw=true "Partitioned data")

In anonymization process a new dataframe is constructed and groups are divided to separate rows by sensitive attribute (education). 

![Anonymized dataset](documents/mondrian_anonymized.png?raw=true "Anonymized data with K=5")

You can test this in practice with: examples/plot_partitions.py 

#### Example: K-Anonymity using DataFrameAnonymizer

    import pandas as pd
    from tabular_anonymizer import DataFrameAnonymizer

    # Setup dataframe
    df = pd.read_csv("./adult.csv", sep=",")
    
    # Define sensitive attributes
    sensitive_columns = ['label']

    # Anonymize dataframe with k=10
    p = MondrianAnonymizer(sensitive_columns)
    df_anonymized = p.anonymize_k_anonymity(df, k=10)

#### Example: K-Anonymity with L-diversity using DataFrameAnonymizer

    import pandas as pd
    from tabular_anonymizer import DataFrameAnonymizer

    # Setup dataframe
    df = pd.read_csv("./adult.csv", sep=",")
    
    # Define sensitive attributes
    sensitive_columns = ['label']

    # Anonymize dataframe with k=10
    p = MondrianAnonymizer(sensitive_columns)
    df_anonymized = p.anonymize_l_diversity(df, k=10, l=2)


### Pseudonymization

Pseudonymization tool is intended for combining data from multiple sources. Both datasets should have common column that
is sensitive information. Combine_and_pseudonymize function 
The user can pseudonymize a direct identifier with a hash function.

![Dataframe before pseudonymization](documents/pseudonymization_before.png?raw=true "Dataframe")

![Dataframe before pseudonymization](documents/pseudonymization_after.png?raw=true "Dataframe after pseudonymization of education column")

![Encryption process](documents/pseudonymization_encryption.png?raw=true "Pseudonymization and ecryption process")



#### Example: Pseudonymization of dataframe column with generated secret key

    from tabular_anonymizer import utils
    file1 = "exampples/adult.csv"
    df = pd.read_csv(file1, sep=",", index_col=0)
    # Simple way
    utils.pseudonymize(df, 'column_name', generate_nonce=True)

#### Example: Pseudonymization of multiple dataframes and columns shared secret key

    # Let's assume we have two dataframes df1 and df2. 
    # Both dataframes have common identifier data in columns column_name1 and column_name2, for example birth date
    # If you want to merge these datasets for example you can encrypt both columns using shared salt before that. 

    from tabular_anonymizer import utils

    # Generate nonces to be used as salt
    nonce1 = utils.create_nonce() # Generated random salt #1
    nonce2 = utils.create_nonce() # Generated random salt #2

    # Pseudonymize given columns using sha3_224 with two salts
    utils.pseudonymize(df1, 'column_name1', nonce1, nonce2)
    utils.pseudonymize(df2, 'column_name2', nonce1, nonce2)

#### Example: Combining two datasets with shared common column with pseudonymization

    # Let's assume that dataframes df1 and df2 have equal size and common column "id" which is direct identifier 
    # (such as phonenumber). We can combine (merge) these two datasets and pseudonymize values in ID-column
    # so it is no longer sensitive information.

    from tabular_anonymizer import utils

    # combine (merge) two datasets with common index column and pseudonymize
    df_c = utils.combine_and_pseudonymize(df1, df2, 'id')

#### Example: Post-processing & partial masking

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

#### More examples

    More examples can be found in examples-folder.

#### Run examples in GitHub Codespaces

Launch new codespace and test example scripts in examples-folder directly in VSCode desktop or web interface.

Tip: If you want to run jupyter-lab server in codespaces, use following command:

        jupyter-lab --ip 0.0.0.0 --config .devcontainer/jupyter-server-config.py --no-browser

Then add port-mapping to port 8888.

#### Run examples in local docker environment

You can run Jupyterlab and do experiments with tabular anonymizer in docker container:

     docker build . -t tabular-anonymizer && docker run --rm -it -p 8888:8888 tabular-anonymizer  

Open http://127.0.0.1:8888 in your web browser and navigate to examples/sample_notebook.ipynb

Hit ctrl + c to quit container.


## Acknowledgements

Mondrian algorithm of this library is based on [glassonion1/AnonyPy](https://github.com/glassonion1/anonypy) mondrian implementation. 

Visualization example (plot_partitions.py) is based on Nuclearstar/K-Anonymity plot implementation. [Nuclearstar/K-Anonymity](https://github.com/Nuclearstar/K-Anonymity)
