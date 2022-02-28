import os
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession, Window, Row


# from schema import *
from pyspark.sql import SparkSession
from sqlalchemy import update
import mysql.connector
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import timedelta
from pyspark.sql.functions import *
from pyspark.sql.types import *

#specify the datapath
data_path = "/home/nam/airflow/dags"
time_zero = timedelta(days= 0, hours=0,minutes=0,seconds =0 )

#read the specific date 
f = open("/home/nam/airflow/dags/data/info.txt", "r")
data_info = f.read().split("|")
Store_Date = data_info[0]
list_status = [data_info[1],data_info[2],data_info[3]]

#create sparkSession
spark = SparkSession \
        .builder \
        .appName("DE_Project4") \
        .config('spark.jars.packages',"mysql:mysql-connector-java:8.0.27")\
        .master("local")\
        .getOrCreate()

#connect to database
mydb = mysql.connector.connect(
host="localhost",
port="3306",
user="root",
password="",
database ="project"
#   database ="project"
)
mycursor = mydb.cursor()
print("Connect sucsess !",mydb)

#create variables for spark connection
database = "project"
user = "root"
password = ""
url = "jdbc:mysql://localhost:3306/project"
driver =  "com.mysql.jdbc.Driver"
properties = {"user":user ,"password":password,"driver":driver}

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")


########################### SCHEMA ########################################################################
transactions_Schema = StructType([StructField("transid",StringType(),True)\
        ,StructField("transStatus",StringType(),True)\
        ,StructField("userId",IntegerType(),True)\
        ,StructField("transactionTime",TimestampType(),True)\
        ,StructField("appId",IntegerType(),True)\
        ,StructField("transType",IntegerType(),True)\
        ,StructField("amount",IntegerType(),True)\
        ,StructField("pmcId",IntegerType(),True)])

Promotions_Schema = StructType([StructField("userid",StringType(),True)\
        ,StructField("voucherCode",StringType(),True)\
        ,StructField("status",StringType(),True)\
        ,StructField("campaignID",IntegerType(),True)\
        ,StructField("time",IntegerType(),True)])


User_Schema = StructType([StructField("userid",StringType(),True)\
        ,StructField("birthdate",DateType(),True)\
        ,StructField("profileLevel",StringType(),True)\
        ,StructField("gender",IntegerType(),True)\
        ,StructField("updatedTime",TimestampType(),True)])

Campaign_Schema = StructType([StructField("campaignID",StringType(),True)\
        ,StructField("campaignType",IntegerType(),True)\
        ,StructField("expireDate",TimestampType(),True)\
        ,StructField("expireTime",IntegerType(),True)])

#############################################################################

#Read dataframe
def read_enity(entity, schema, Store_Date, delimiter='\t', header='True'):
            return spark.read.format("csv").schema(schema)\
            .options(header=header, delimiter=delimiter) \
            .load(data_path+"/data/source/"+entity+"/"+Store_Date)

#Read and update user informations to table user_info in database
def updateUser():
    user = read_enity("users", User_Schema, Store_Date)
    print("Load users succsessfully")
    user.write.mode("append").parquet("/home/nam/airflow/dags/data/datalake/user/"+Store_Date)
    print("Write users to datalake successfully!")
    user = (user.withColumn("updatedTime", date_trunc("second", col("updatedTime"))).withColumn("birthdate", to_date(col("birthdate"))) )
    w = Window.partitionBy(user.userid).orderBy(user.updatedTime.desc())
    user_info = user.withColumn("row_number", row_number().over(w)).filter(col("row_number")==1).drop("row_number")

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
                print("PASS")
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

#Read and update transactions informations to table payment, active, userApp, userPmc in database
def updateTransactions():
    transactions = read_enity("transactions",transactions_Schema,Store_Date)
    print("Load Transactions succsessfully")
    transactions = transactions.filter("transStatus = 1").withColumn("transactionTime", date_trunc("second",col("transactionTime")))
    # transactions.show(10)
    transactions.write.mode("append").parquet("/home/nam/airflow/dags/data/datalake/transactions/" + Store_Date)
    print("Write transactions to datalake successfully!")

    ############################### Payment ###################################################################
    paymentDate = (transactions
        .filter("transType = 3")
        .groupBy("userId")
        .agg(min("transactionTime").alias("fistPaymentDate"), max("transactionTime").alias("transactionTime")))

    payment = (paymentDate.join(transactions.filter("transType = 3"), ["userId", "transactionTime"])
        .withColumnRenamed("transactionTime", "lastPaymentDate")
        .withColumnRenamed("appId", "lastPaymentApp")
        .select("userId", "fistPaymentDate", "lastPaymentDate", "lastPaymentApp"))

    # payment.show(10)

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

    ###########################App List ############################
    userApp = transactions.select("userId", "appId").distinct()
    # userApp.show(5)
    list_row_userApp = userApp.collect()
    def config_update_userApp(row):
        userId = row.__getitem__("userId")
        appId = row.__getitem__("appId")
        sql_query = f"SELECT * FROM userApp where userId = '{userId}' AND appId = '{appId}' "
        mycursor.execute(sql_query)
        result = mycursor.fetchall()
        number_sameID = len(result)
        insertVal = (userId, appId)
        if number_sameID == 0:
            print("INSERT ",userId,appId)
            option = "INSERT"
            queryInsert = f""" 
            {option} INTO userApp(userId, appId) VALUES (%s,%s) """
            mycursor.execute(queryInsert, insertVal)
            mydb.commit()
        else: 
            print("PASS",userId,appId)
            pass

    for row in list_row_userApp:
        config_update_userApp(row)
    print("WRITE UserApp TO DATABASES SUCSSESFULL !")
    ###########################Source payment############################

    userPmc = transactions.select("userId", "pmcId").distinct()
    # userPmc.show(5)
        # userPmc.show(5)
    list_row_userPmc = userPmc.collect()
    def config_update_userPmc(row):
        userId = row.__getitem__("userId")
        pmcId = row.__getitem__("pmcId")
        sql_query = f"SELECT * FROM userPmc where userId = '{userId}' AND pmcId = '{pmcId}' "
        mycursor.execute(sql_query)
        result = mycursor.fetchall()
        number_sameID = len(result)
        insertVal = (userId, pmcId)
        if number_sameID == 0:
            option = "INSERT"
            print("INSERT",userId,pmcId)
            queryInsert = f""" 
            {option} INTO userPmc(userId, pmcId) VALUES (%s,%s) """
            mycursor.execute(queryInsert, insertVal)
            mydb.commit()
        else: 
            print("PASS")
            pass

    for row in list_row_userPmc:
        config_update_userPmc(row)
    print("WRITE userPmc TO DATABASES SUCSSESFULL !")
#Read and update Promotions informations to table userPromotion
def updatePromotions():
    prom = read_enity("promotions", Promotions_Schema, Store_Date)
    print("LOAD promotion succsessfully")
    prom.write.mode("append").parquet("/home/nam/airflow/dags/data/datalake/promotion/" + Store_Date)
    print("Write promotions to datalake successfully!")

    def read_enity_config(entity, schema,  delimiter='\t', header='True'):
            return spark.read.format("csv").schema(schema).options(header=header, delimiter=delimiter) \
                .load(data_path+"/data/source/"+entity)
    camp = read_enity_config("configs", Campaign_Schema)
    print("LOAD campaign succsessfully")

    ###############################Promotions############################
    promotion = (prom.join(camp, "campaignID")
    .withColumn("voucherExpire", (col("time") + col("expireTime")).cast("timestamp"))
    .withColumn("time", col("time").cast("timestamp")))

    userPromotion = (promotion.withColumn("actualExpire", when((col("campaignType")=="2") & 
                                                              (col("expireDate") > col("voucherExpire")), col("voucherExpire"))
                     .otherwise(col("expireDate")))
    .withColumn("Expire", current_date() > col("actualExpire"))
    .select("userid", "campaignID", "status", "Expire"))
    userPromotion = userPromotion.distinct()
    
    list_row_userPromotion = userPromotion.collect()

    def config_update_userPromotion(row):
        userid = row.__getitem__("userid")
        campaignID = row.__getitem__("campaignID")
        status = row.__getitem__("status")
        Expire = row.__getitem__("Expire")

        sql_query = f"SELECT * FROM userPromotion where userid = '{userid}' AND campaignID = '{campaignID}' "
        mycursor.execute(sql_query)
        result = mycursor.fetchall()
        number_sameID = len(result)
        insertVal = (userid, campaignID, status, Expire)
        updateVal = (status, Expire, userid, campaignID)
        if number_sameID > 0:
            print("UPDATE",userid,campaignID)
            option = "UPDATE"
            queryUpdate = f"""
            {option} userPromotion SET status=%s , Expire=%s
            WHERE userid = %s and campaignID = %s
            """
            mycursor.execute(queryUpdate, updateVal)
            mydb.commit()
        else: 
            option = "INSERT"
            queryInsert = f""" 
            {option} INTO userPromotion(userid, campaignID, status, Expire) VALUES (%s,%s,%s,%s) """
            mycursor.execute(queryInsert, insertVal)
            mydb.commit()
    for row in list_row_userPromotion:
        config_update_userPromotion(row)
    print("WRITE User Promotions TO DATABASES SUCSSESFULL ! ")

if list_status[0]=="1":
    updateUser()
else: 
    print("No data of user in date: ", Store_Date)
    pass

if list_status[1]=="1":
    updateTransactions()
else: 
    print("No data of Transactions in date: ", Store_Date)
    pass

if list_status[2]=="1":
    updatePromotions()
else: 
    print("No data of Promotions in date: ", Store_Date)
    pass


