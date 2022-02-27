import os
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession, Window, Row
from pendulum import yesterday
from functools import reduce

# import pandas as pd
# from schema import *

# from airflow import DAG


Store_Date = '2021-11-01'

spark = SparkSession \
    .builder \
    .appName("DE_Project4") \
    .getOrCreate()

entity = 'users'
User_Schema = StructType([StructField("userid", StringType(), True), StructField("birthdate", DateType(), True), StructField(
    "profileLevel", StringType(), True), StructField("gender", IntegerType(), True), StructField("updatedTime", TimestampType(), True)])

user = spark.read.format("csv").schema(User_Schema).options(header='True', delimiter='\t') \
    .load("hdfs://namenode:9000/data/source/"+entity+"/"+Store_Date)
print("LOAD SUCSESSS", entity)

entity = 'transactions'
transactions_Schema = StructType([StructField("transid", StringType(), True), StructField("transStatus", StringType(), True), StructField("userId", IntegerType(), True), StructField(
    "transactionTime", TimestampType(), True), StructField("appId", IntegerType(), True), StructField("transType", IntegerType(), True), StructField("amount", IntegerType(), True), StructField("pmcId", IntegerType(), True)])

transactions = spark.read.format("csv").schema(transactions_Schema).options(header='True', delimiter='\t') \
    .load("hdfs://namenode:9000/data/source/"+entity+"/"+Store_Date)
print("LOAD SUCSESSS", entity)

entity = 'promotions'
Promotions_Schema = StructType([StructField("userid", StringType(), True), StructField("voucherCode", StringType(), True), StructField(
    "status", StringType(), True), StructField("campaignID", IntegerType(), True), StructField("time", TimestampType(), True)])

promotions = spark.read.format("csv").schema(transactions_Schema).options(header='True', delimiter='\t') \
    .load("hdfs://namenode:9000/data/source/"+entity+"/"+Store_Date)


print("LOAD SUCSESSS", entity)
