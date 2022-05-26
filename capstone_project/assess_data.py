# Import Necessary Libraries
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql import SQLContext
from pyspark.sql.functions import isnan, when, count, col, udf, dayofmonth, dayofweek, month, year, weekofyear
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import *


def general_dataframe_info(df):
    print ("-----General Information on the Dataframe-----")
    print ("Dataframe Shape:", df.shape, "\n")
    print ("Column Headers:", list(df.columns.values), "\n")
    print (df.dtypes)
    
def dataframe_report(df):
    import re
    
    missing = []

    print("Unique Values")
    
    for column in df:
        # report unique values for each features
        values = df[column].unique() 
        st = "{} has {} unique values".format(column, values.size)
        print(st)
        
            
        # report missing values for each features
        if (True in pd.isnull(values)):
            percentage = 100 * pd.isnull(df[column]).sum() / df.shape[0]
            s = "{} is missing in {}, {}%.".format(pd.isnull(df[column]).sum(), column, percentage)
            missing.append(s)
            
    print ("\n--------------------------------------------------------------\n")
    print("Missing Values")
    
    print ("Features that contain missing values:")
    for i in range(len(missing)):
        print("\n{}" .format(missing[i]))

def visualise_missing_records(df):
    """
    visualising missing values by columns
    """
    # discover missing record per column
    missing_df = pd.DataFrame(data= df.isnull().sum(), columns=['values'])
    missing_df = missing_df.reset_index()
    missing_df.columns = ['cols', 'values']

    # calculate % proportion of missing records
    missing_df['% of missing records'] = 100*missing_df['values']/df.shape[0]
    
    plt.rcdefaults()
    sns.set_style("darkgrid", {"axes.facecolor": ".9"})
    sns.set_context("paper")
    plt.subplots(figsize=(10,5))
    ax = sns.barplot(x="cols", y="% of missing records", data=missing_df, palette="Blues_d")
    ax.set_ylim(0, 100)
    ax.set_xticklabels(ax.get_xticklabels(), rotation=90)
    plt.show()