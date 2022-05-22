from pyspark.sql import SparkSession, SQLContext, GroupedData
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql.functions import isnan, when, count, col, udf, dayofmonth, dayofweek, month, year, weekofyear
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import avg
from glob import glob
import pandas as pd
import numpy as np
import re
import configparser
import matplotlib.pyplot as plt
import seaborn as sns
import assess_data as assess
import clean_data as clean
import create_schema as schema
import datetime, time
from datetime import datetime, timedelta
from pyspark.sql import types as T


def dim_visa(df, output_data):
    """
        Gather visa data, create dataframe and write data into parquet files.
        
        :param df: dataframe of input data.
        :param output_data: path to write data to.
        :return: dataframe representing visa dimension
    """
    
    dim_visa = df.withColumn('visa_id', monotonically_increasing_id()) \
                .select(['visa_id','i94visa', 'visatype', 'visapost']) \
                .dropDuplicates(['i94visa', 'visatype', 'visapost'])
    
    # write dimension to parquet file
    dim_visa.write.parquet(output_data, mode="overwrite")
    
    return dim_visa

def dim_demographics(df, output_data):
    """This function creates a us demographics dimension table from the us cities demographics data.
    
    :param df: spark dataframe of us demographics survey data
    :param output_data: path to write dimension dataframe to
    :return: spark dataframe representing demographics dimension
    """
    dim_demographics = df.withColumn('id', monotonically_increasing_id()) \
            .withColumnRenamed('Median Age','median_age') \
            .withColumnRenamed('Male Population', 'male_population') \
            .withColumnRenamed('Female Population', 'female_population') \
            .withColumnRenamed('Total Population', 'total_population') \
            .withColumnRenamed('Number of Veterans', 'number_of_veterans') \
            .withColumnRenamed('Foreign-born', 'foreign_born') \
            .withColumnRenamed('Average Household Size', 'average_household_size') \
            .withColumnRenamed('State Code', 'state_code')
    
    # write dimension to parquet file
    dim_demographics.write.parquet(output_data, mode="overwrite")
    
    return dim_demographics

def dim_time(df, output_data):
    """
        Gather time data, create dataframe and write data into parquet files.
        
        :param df: dataframe of input data.
        :param output_data: path to write data to.
        :return: dataframe representing time dimension
    """
     
    def convert_datetime(t):
        try:
            start = datetime(1960, 1, 1)
            return start + timedelta(days=int(t))
        except:
            return None
    
    udf_datetime = udf(lambda x: convert_datetime(x), T.DateType())

    dim_time = df.select(["arrdate"])\
                .withColumn("arrival_date", udf_datetime("arrdate")) \
                .withColumn('day', F.dayofmonth('arrival_date')) \
                .withColumn('month', F.month('arrival_date')) \
                .withColumn('year', F.year('arrival_date')) \
                .withColumn('week', F.weekofyear('arrival_date')) \
                .withColumn('weekday', F.dayofweek('arrival_date'))\
                .select(["arrdate", "arrival_date", "day", "month", "year", "week", "weekday"])\
                .dropDuplicates(["arrdate"])
    
    dim_time.write.parquet(output_data, mode="overwrite")
    
    return dim_time