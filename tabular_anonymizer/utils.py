import hashlib
import string
import random
from typing import List

import pandas as pd
from pandas import DataFrame


def __encrypt(x, n1, n2):
    s = n1 + str(x) + n2
    hash = hashlib.sha3_224(s.encode()).hexdigest()
    return hash


def prepare_index_for_anonymization(df: DataFrame):
    df.reset_index()
    df.index = range(len(df))


def pseudonymize(df: DataFrame, column: str, nonce1=None, nonce2=None, generate_nonce=False):
    if not nonce1 or len(nonce1) < 10:
        if not generate_nonce:
            raise Exception("Give nonce1, at least 10 characters long.")
        else:
            nonce1 = create_nonce()
    if not nonce2 or len(nonce2) < 10:
        if not generate_nonce:
            raise Exception("Give nonce2, at least 10 characters long.")
        else:
            nonce2 = create_nonce()

    s = df[column].apply(lambda row: __encrypt(row, nonce1, nonce2))
    df[column] = s
    df[column] = df[column].astype("category")


def create_nonce(size=10):
    nonce = ''.join(random.choice(string.hexdigits) for i in range(size))
    return nonce


def combine_and_pseudonymize(df1: DataFrame, df2: DataFrame, column: str, nonce1=None, nonce2=None):
    if not nonce1 or len(nonce1) < 10:
        nonce1 = create_nonce()
    if not nonce2 or len(nonce2) < 10:
        nonce2 = create_nonce()
    pseudonymize(df1, column, nonce1, nonce2)
    pseudonymize(df2, column, nonce1, nonce2)
    return pd.merge(df1, df2, on=column)


def format_column_as_zipcode(df, column_name):
    df[column_name] = df[column_name].astype(str).str.zfill(5)
    df[column_name] = df[column_name].astype("category")


# Helper method, convert datetime to year
def convert_datetime_to_year(df, column_name):
    df[column_name] = pd.to_datetime(df[column_name])
    df[column_name] = pd.DatetimeIndex(df[column_name]).year
    df[column_name] = df[column_name].astype("number")


# Post-generalization helper
def generalize(df: DataFrame, column_name: str, method: staticmethod):
    df[column_name] = df[column_name].apply(lambda x: method(x))


# Handling for zip codes and other numeric categorical columns
def mark_as_categorical(df, categorical_columns: List[str]):
    for name in categorical_columns:
        df[name] = df[name].astype("category")


# Converts intervals to partially masked, eg ['20220', '20210'] => '202**'
def generalize_partial_masking(value_array):
    if value_array:
        if isinstance(value_array, str):
            return value_array
        if isinstance(value_array, list):
            value_array = [x for x in value_array if x is not None]

            if len(value_array) == 1:
                value_array = value_array[0].split(',')

            if len(value_array) == 1:
                return value_array[0]

            value_array.sort()
            min_l = 0
            # Find shortest word
            res = []
            generalized = []
            for val in value_array:
                l: int = len(val)
                # cut words to min length
                if l < min_l or min_l == 0:
                    min_l = l
                res.append(list(val))
            # cell iteration
            ci = 0
            match = True
            while ci < min_l:
                ri: int = 1
                # row iteration
                a = res[0][ci]
                if match:
                    while ri < len(res):
                        b = res[ri][ci]
                        if a != b:
                            match = False
                        ri += 1

                if match:
                    generalized.append(a)
                else:
                    generalized.append('*')
                ci += 1
            generalized = ''.join(generalized)
            return generalized
        return None