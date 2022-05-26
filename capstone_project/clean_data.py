# Import Necessary Libraries
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql import SQLContext
from pyspark.sql.functions import isnan, when, count, col, udf, dayofmonth, dayofweek, month, year, weekofyear
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import *

def remove_missing_data(df):
    """
        Clean the data within the specified dataframe.
    """
    print("Missing data is being removed...")
    
    columns_to_drop = []
    
    for column in df:
        values = df[column].unique() 
        
        # find all missing records in every features
        if (True in pd.isnull(values)):
            percentage_missing = 100 * pd.isnull(df[column]).sum() / df.shape[0]
            
            if (percentage_missing >= 90):
                columns_to_drop.append(column)
    
    # remove all columns that where more than 90% of data is missing
    df = df.drop(columns=columns_to_drop)

    # remove all rows with missing records
    df = df.dropna(how='all')
    
    print("Removal of missing data completed")
    
    return df

def remove_duplicate_rows(df, cols=[]):
    """
        Remove duplicate data (rows) within the specified dataframe.
    """
    print("Removing duplicate rows...")
    row_count_before = df.shape[0]
    # Remove duplicated rows
    df = df.drop_duplicates()
    print("{} rows removed.".format(row_count_before - df.shape[0]))
    return df
