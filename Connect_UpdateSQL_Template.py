
from cProfile import Profile
from email.utils import format_datetime
from wsgiref.handlers import format_date_time
from pandas import DataFrame
from pyspark.sql import SparkSession
from sqlalchemy import update
from schema import *
import mysql.connector
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import timedelta

######### Connect To DATABASE ################
mydb = mysql.connector.connect(
  host="localhost",
  port="3306",
  user="root",
  password="",
  database ="project"
)

print("Connect sucsess !",mydb)

spark = SparkSession \
    .builder \
    .appName("DE_Project4") \
    .config('spark.jars.packages',"mysql:mysql-connector-java:8.0.27")\
    .master("local")\
    .getOrCreate()

database = "project"
user = "root"
password = ""
url = "jdbc:mysql://localhost:3306/project"
driver =  "com.mysql.jdbc.Driver"
properties = {"user":user ,"password":password,"driver":driver}

mycursor = mydb.cursor()

from functools import reduce
from pendulum import yesterday
from pyspark import SparkContext
from pyspark.sql import SparkSession, Window, Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
# import pandas as pd
from schema import *



### Read Write to dataframe ###############
Store_Date =  '2021-11-03'

def read_enity(entity, schema, Store_Date, delimiter='\t', header='True'):
        return spark.read.format("csv").schema(schema).options(header=header, delimiter=delimiter) \
            .load("./data/source/"+entity+"/"+Store_Date)
try:
    user = read_enity("users", User_Schema, Store_Date)
    print("Load users succsessfully")
    # user.write.mode("append").parquet("./data/datalake/user/"+Store_Date)
except Exception as e:
     print("ERROR:",e)       
user = (user
       .withColumn("updatedTime", date_trunc("second", col("updatedTime")))
        .withColumn("birthdate", to_date(col("birthdate"))) )

w = Window.partitionBy(user.userid).orderBy(user.updatedTime.desc())
user_info = user.withColumn("row_number", row_number().over(w)).filter(col("row_number")==1).drop("row_number")
user_info.orderBy("userid").show(10)
# user_info.write.mode("overwrite").jdbc(url=url,table="user_info",properties=properties)


######################################################

list_row = user_info.collect()

def config_update(row ):

    user_id = row.__getitem__("userid")
    birthdate = row.__getitem__("birthdate")
    ProfileLevel = row.__getitem__("profileLevel")
    gender = row.__getitem__("gender")
    updatedTime =  row.__getitem__("updatedTime")

    ## Check in DataFrame if record exists
    sql_query = f"SELECT * FROM user_info where userid = '{user_id}' "
    mycursor.execute(sql_query)
    result = mycursor.fetchall()
    number_sameID = len(result)
    val = (user_id,birthdate,ProfileLevel,gender,updatedTime)
    val2=(birthdate,ProfileLevel,gender,updatedTime,user_id)

    if number_sameID > 0:
        date_in_data_in_DB = result[0][4]
        option = "UPDATE"
        print("[ DB:",date_in_data_in_DB,"|","root:",updatedTime,"]")
        diff_time =  updatedTime - date_in_data_in_DB 
        print(diff_time)
        if diff_time <= timedelta(days = 0, hours=0,minutes=0,seconds =0 ) :
            pass
        else:
            print("Update ID",user_id)
            query = f"""
            {option} user_info SET birthdate=%s , ProfileLevel=%s , gender=%s ,updatedTime =%s
            WHERE userid = %s
            """
            mycursor.execute(query,val2)
            mydb.commit()

    else:
        option = "INSERT"
        query = f""" 
        {option} INTO user_info(userid,birthdate,profileLevel,gender,updatedTime) VALUES (%s,%s,%s,%s,%s) """
        mycursor.execute(query,val)
        mydb.commit()
        print("INSERT NEW ID ",user_id)

    
for row in list_row:
    config_update(row)



   

# print(result)
