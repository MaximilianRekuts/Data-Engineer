from pyspark.sql import SparkSession, SQLContext, GroupedData
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F
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
import os
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
    
    dim_visa = df.withColumn('visa_id', monotonically_increasing_id()) \
                .select(['visa_id','i94visa', 'visatype', 'visapost']) \
                .dropDuplicates(['i94visa', 'visatype', 'visapost'])
    
    # write dimension to parquet file
    dim_visa.write.parquet(output_data, mode="overwrite")
    print('visa table created')
    
    return dim_visa


def dim_demographics(df, output_data):

    dim_demographics = df.withColumn('id', monotonically_increasing_id()) \
            .withColumnRenamed('Median Age','median_age') \
            .withColumnRenamed('Male Population', 'male_population') \
            .withColumnRenamed('Female Population', 'female_population') \
            .withColumnRenamed('Total Population', 'total_population') \
            .withColumnRenamed('Number of Veterans', 'number_of_veterans') \
            .withColumnRenamed('Foreign-born', 'foreign_born') \
            .withColumnRenamed('Average Household Size', 'average_household_size') \
            .withColumnRenamed('State Code', 'state_code') \
            .select(['id','median_age','male_population', 'female_population', 'total_population', 'number_of_veterans', 'foreign_born', 'average_household_size', 'state_code']) 
    
    # write dimension to parquet file
    dim_demographics.write.parquet(output_data + "demographics", mode="overwrite")
    print('demographics table created')
    
    return dim_demographics

def dim_time(df, output_data):
     
    def convert_datetime(t):
        try:
            start = datetime(1960, 1, 1)
            return start + timedelta(days=int(t))
        except:
            return None
    
    udf_datetime = udf(lambda x: convert_datetime(x), T.DateType())

    dim_time = df.select(["arrdate"])\
                .withColumn("arrival_date", udf_datetime("arrdate")) \
                .withColumn('day', F.dayofmonth('arrival_date'))\
                .withColumn('week', F.weekofyear('arrival_date'))\
                .withColumn('weekday', F.dayofweek('arrival_date'))\
                .withColumn('month', F.month('arrival_date')) \
                .withColumn('year', F.year('arrival_date')) \
                .select(["arrdate", "arrival_date", "day", "month", "year", "week", "weekday"])\
                .dropDuplicates(["arrdate"])
    
    dim_time.write.parquet(output_data, mode="overwrite")
    print('time table created')
    
    return dim_time

def create_country_temperature(temperature_spark, country_spark, output_data):
    
    dim_temperature = temperature_spark.groupBy(col("Country").alias("country")).agg(
                mean('AverageTemperature').alias("average_temperature"),\
                mean("AverageTemperatureUncertainty").alias("average_temperature_uncertainty")
                ).dropna()\
                .withColumn("temperature_id", monotonically_increasing_id()) \
                .select(["temperature_id", "country", "average_temperature", "average_temperature_uncertainty"])

    dim_temperature.write.parquet(output_data + "temperature", mode='overwrite')
    
    dim_country = country_spark
    
    dim_country.write.parquet(output_data + "country", mode = 'overwrite')
    
    # join country and temperature
    dim_country_temperature = dim_country.select(["*"])\
            .join(dim_temperature, (dim_country.Name == upper(dim_temperature.country)), how='full')\
            .select([dim_country.code, dim_country.Name, dim_temperature.temperature_id, dim_temperature.average_temperature, dim_temperature.average_temperature_uncertainty])
    
    dim_country_temperature.write.parquet(output_data + "country_temperature", mode='overwrite')
    print('country_temperature table created')
    
    return dim_country_temperature


def create_immigration_fact(immigration_spark, dim_visa, dim_time, dim_demographics, dim_country_temperature, output_data, spark):

    immigration_spark = immigration_spark.limit(10000)
    # join all tables to immigration
    df = immigration_spark.select(["*"])\
                .join(dim_visa, (immigration_spark.i94visa == dim_visa.i94visa) & (immigration_spark.visatype == dim_visa.visatype)\
                      & (immigration_spark.visapost == dim_visa.visapost), how ='left')\
                .join(dim_time, (immigration_spark.arrdate == dim_time.arrdate), how ='left')\
                .join(dim_demographics, (immigration_spark.i94res == dim_demographics.state_code), how = 'left')\
                .join(dim_country_temperature, (immigration_spark.i94res == dim_country_temperature.code), how = 'left')\
                .where(col('cicid').isNotNull())\
                .select(["cicid", "i94res", "depdate", "i94mode", "i94port", "i94cit", "i94addr", "airline", "fltno", "entdepa", "entdepd",\
                          "visa_id", "temperature_id", "gender", "state_code", dim_time.arrdate.alias("arrdate"), "admnum", "dtaddto", "biryear"])


    df.write.parquet(os.path.join(output_data, 'test'), 'overwrite')
    print('fact immigration table created')
    
    return df


def checks(df, table_name):

    total_count = df.count()

    if total_count == 0:
        print(f"Qualify check has failed for {table_name} with 0 values!")
    else:
        print(f"Qualify check has passed for {table_name} with {total_count:,} values.")
    return 0