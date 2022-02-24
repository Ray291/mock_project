
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

################################ Connect To DATABASE ########################
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



####################################### Read Write to dataframe #############################################
Store_Date =  '2021-11-01'
time_zero = timedelta(days = 0, hours=0,minutes=0,seconds =0 ) 
def read_enity(entity, schema, Store_Date, delimiter='\t', header='True'):
        return spark.read.format("csv").schema(schema).options(header=header, delimiter=delimiter) \
            .load("./data/source/"+entity+"/"+Store_Date)

user = read_enity("users", User_Schema, Store_Date)
print("Load users succsessfully")
# user.write.mode("append").parquet("./data/datalake/user/"+Store_Date)
     
user = (user.withColumn("updatedTime", date_trunc("second", col("updatedTime"))).withColumn("birthdate", to_date(col("birthdate"))) )


transactions = read_enity("transactions",transactions_Schema,Store_Date)
print("Load Transactions succsessfully")
transactions = transactions.filter("transStatus = 1").withColumn("transactionTime", date_trunc("second",col("transactionTime")))
# transactions.show(10)


w = Window.partitionBy(user.userid).orderBy(user.updatedTime.desc())
user_info = user.withColumn("row_number", row_number().over(w)).filter(col("row_number")==1).drop("row_number")


# user_info.orderBy("userid").show(10)
# user_info.write.mode("overwrite").jdbc(url=url,table="user_info",properties=properties)


###################################################### user_info ############################################

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

print("WRITE user_info TO DATABASES SUCSSESFULL ! ")

############################### Payment ###################################################################

paymentDate = (transactions
    .filter("transType = 3")
    .groupBy("userId")
    .agg(min("transactionTime").alias("fistPaymentDate"), max("transactionTime").alias("transactionTime")))

payment = (paymentDate.join(transactions.filter("transType = 3"), ["userId", "transactionTime"])
    .withColumnRenamed("transactionTime", "lastPaymentDate")
    .withColumnRenamed("appId", "lastPaymentApp")
    .select("userId", "fistPaymentDate", "lastPaymentDate", "lastPaymentApp"))

payment.show(10)

list_row_payment = payment.collect()

def config_update_payment(row):
    userId = row.__getitem__("userId")
    fistPaymentDate = row.__getitem__("fistPaymentDate")
    lastPaymentDate = row.__getitem__("lastPaymentDate")
    lastPaymentApp = row.__getitem__("lastPaymentApp")

    sql_query = f"SELECT * FROM payment where userId = '{userId}' "
    mycursor.execute(sql_query)
    result = mycursor.fetchall()

    number_sameID = len(result)
   
    udateVal = (fistPaymentDate, userId)
    udateVal2 = (lastPaymentDate, lastPaymentApp, userId)
    insertVal = (userId, fistPaymentDate, lastPaymentDate, lastPaymentApp)

    if number_sameID > 0:
        option = "UPDATE"
        fistPaymentDateDB = result[0][1]
        lastPaymentDateDB = result[0][2]
        #fistPaymentDate must smaller than fistPaymentDateDB to satisfy the requirement to update
        compare1stPayment = fistPaymentDate - fistPaymentDateDB
        #lastPaymentDate must greater than lastPaymentDateDB to satisfy the requirement to update
        compareLastPayment = lastPaymentDate - lastPaymentDateDB
        #lastPaymentDate <= lastPaymentDateDB
        if compare1stPayment < time_zero:
            print("Update First Payment ",userId)
            print("[ DB:",fistPaymentDateDB,"|","newDate:",fistPaymentDate,"]")
            print(compare1stPayment)
            queryUpdate = f"""
            {option} payment SET fistPaymentDate=%s
            WHERE userId = %s
            """
            mycursor.execute(queryUpdate, udateVal)
            mydb.commit()
        elif compareLastPayment > time_zero :
            print("Update Last Payment",userId)
            print("[ DB:",lastPaymentDateDB,"|","newDate:",lastPaymentDate,"]")
            queryUpdate = f"""
            {option} payment SET  lastPaymentDate=%s , lastPaymentApp=%s
            WHERE userId = %s
            """
            mycursor.execute(queryUpdate, udateVal2)
            mydb.commit()
        else:
            pass 

    else: 
        option = "INSERT"
        queryInsert = f""" 
        {option} INTO payment(userId, fistPaymentDate, lastPaymentDate, lastPaymentApp) VALUES (%s,%s,%s,%s) """
        mycursor.execute(queryInsert, insertVal)
        mydb.commit()
        print("INSERT",userId)
        
for row in list_row_payment:
    config_update_payment(row)

print("WRITE PAYMENT TO DATABASES SUCSSESFULL ! ")
    


####################################### ACTIVE #################################

activeDate = (transactions.groupBy("userId")
    .agg(min("transactionTime").alias("fistActiveDate"), max("transactionTime").alias("transactionTime")))

active = (activeDate.join(transactions, ["userId", "transactionTime"])
    .withColumnRenamed("transactionTime", "lastActiveDAte")
    .withColumnRenamed("appId", "lastApp")
    .withColumnRenamed("transType", "lastTransType")
    .select("userId", "fistActiveDate", "lastActiveDAte", "lastTransType"))

# active.show(5)

list_row_active = active.collect()

def config_update_active(row):
    userId = row.__getitem__("userId")
    fistActiveDate = row.__getitem__("fistActiveDate")
    lastActiveDate = row.__getitem__("lastActiveDAte")
    lastTransType = row.__getitem__("lastTransType")

    sql_query = f"SELECT * FROM active where userId = '{userId}' "
    mycursor.execute(sql_query)
    result = mycursor.fetchall()

    number_sameID = len(result)
   
    udateVal = (fistActiveDate, userId)
    udateVal2 = (lastActiveDate, lastTransType, userId)
    insertVal = (userId, fistActiveDate, lastActiveDate, lastTransType)

    if number_sameID > 0:
        option = "UPDATE"
        fistActiveDateDB = result[0][1]
        lastActiveDateDB = result[0][2]
        #fistActiveDate must smaller than fistActiveDateDB to satisfy the requirement to update
        compare1stActive = fistActiveDate - fistActiveDateDB
        #lastActiveDate must greater than lastActiveDateDB to satisfy the requirement to update
        compareLastActive = lastActiveDate - lastActiveDateDB
        #lastActiveDate <= lastActiveDateDB
        if compare1stActive < time_zero:
            print("Update First Active ",userId)
            print("[ DB:",fistActiveDateDB,"|","newDate:",fistActiveDate,"]")
            print(compare1stActive)
            queryUpdate = f"""
            {option} active SET fistActiveDate=%s
            WHERE userId = %s
            """
            mycursor.execute(queryUpdate, udateVal)
            mydb.commit()
        elif compareLastActive > time_zero :
            print("Update Last Active",userId)
            print("[ DB:",lastActiveDateDB,"|","newDate:",lastActiveDate,"]")
            queryUpdate = f"""
            {option} active SET  lastActiveDAte=%s , lastTransType=%s
            WHERE userId = %s
            """
            mycursor.execute(queryUpdate, udateVal2)
            mydb.commit()
        else:
            pass 

    else: 
        option = "INSERT"
        queryInsert = f""" 
        {option} INTO active(userId, fistActiveDate, lastActiveDate, lastTransType) VALUES (%s,%s,%s,%s) """
        mycursor.execute(queryInsert, insertVal)
        mydb.commit()
        print("INSERT",userId)
        
    
        
for row in list_row_active:
    config_update_active(row)
    
print("WRITE ACTIVE TO DATABASES SUCSSESFULL ! ")


