from pyspark.sql import SparkSession, SQLContext, GroupedData
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType, FloatType
from glob import glob
import pandas as pd
import numpy as np
import re
import configparser
import matplotlib.pyplot as plt
import seaborn as sns
import assess_data as assess
import clean_data as clean


def create_temperature_schema(clean_df, spark):
        schema = StructType([StructField("dt", StringType(), True)\
                          ,StructField("AverageTemperature", FloatType(), True)\
                          ,StructField("AverageTemperatureUncertainty", FloatType(), True)\
                          ,StructField("City", StringType(), True)\
                          ,StructField("Country", StringType(), True)\
                          ,StructField("Latitude", StringType(), True)\
                          ,StructField("Longitude", StringType(), True)])

        clean_df = clean_df.loc[:1000]
        temperature_schema = spark.createDataFrame(clean_df, schema=schema)
        
        
        print('temperature schema created')
        
        return(temperature_schema)

def create_airport_schema(clean_df, spark):
        schema = StructType([StructField("ident", StringType(), True)\
                        ,StructField("type", StringType(), True)\
                        ,StructField("name", StringType(), True)\
                        ,StructField("elevation_ft", FloatType(), True)\
                        ,StructField("continent", StringType(), True)\
                        ,StructField("iso_country", StringType(), True)\
                        ,StructField("iso_region", StringType(), True)\
                        ,StructField("municipality", StringType(), True)\
                        ,StructField("gps_code", StringType(), True)\
                        ,StructField("iata_code", StringType(), True)\
                        ,StructField("local_code", StringType(), True)\
                        ,StructField("coordinates", StringType(), True)])

        airport_schema = spark.createDataFrame(clean_df, schema=schema)
        
        print('airport schema created')
        
        return(airport_schema)

def create_demographics_schema(clean_df, spark):
        schema= StructType([StructField("City", StringType(), True)\
                        ,StructField("State", StringType(), True)\
                        ,StructField("Median Age", FloatType(), True)\
                        ,StructField("Male Population", FloatType(), True)\
                        ,StructField("Female Population", FloatType(), True)\
                        ,StructField("Total Population", IntegerType(), True)\
                        ,StructField("Number of Veterans", FloatType(), True)\
                        ,StructField("Foreign-born", FloatType(), True)\
                        ,StructField("Average Household Size", FloatType(), True)\
                        ,StructField("State Code", StringType(), True)\
                        ,StructField("Race", StringType(), True)\
                        ,StructField("Count", IntegerType(), True)])

        demographics_schema = spark.createDataFrame(clean_df, schema=schema)
        
        print('demographics schema created')       
        
        return(demographics_schema)

def create_immigration_schema(clean_df, spark):

        schema = StructType([StructField("cicid", FloatType(), True)\
                          ,StructField("i94yr", FloatType(), True)\
                          ,StructField("i94mon", FloatType(), True)\
                          ,StructField("i94cit", FloatType(), True)\
                          ,StructField("i94res", FloatType(), True)\
                          ,StructField("i94port", StringType(), True)\
                          ,StructField("arrdate", FloatType(), True)\
                          ,StructField("i94mode", FloatType(), True)\
                          ,StructField("i94addr", StringType(), True)\
                          ,StructField("depdate", FloatType(), True)\
                          ,StructField("i94bir", FloatType(), True)\
                          ,StructField("i94visa", FloatType(), True)\
                          ,StructField("count", FloatType(), True)\
                          ,StructField("dtadfile", StringType(), True)\
                          ,StructField("visapost", StringType(), True)\
                          ,StructField("entdepa", StringType(), True)\
                          ,StructField("entdepd", StringType(), True)\
                          ,StructField("matflag", StringType(), True)\
                          ,StructField("biryear", FloatType(), True)\
                          ,StructField("dtaddto", StringType(), True)\
                          ,StructField("gender", StringType(), True)\
                          ,StructField("airline", StringType(), True)\
                          ,StructField("admnum", FloatType(), True)\
                          ,StructField("fltno", StringType(), True)\
                          ,StructField("visatype", StringType(), True)])
        
        clean_df = clean_df.loc[:10000]
        immigration_schema = spark.createDataFrame(clean_df, schema=schema)

        print('immigration schema created')

        return(immigration_schema)