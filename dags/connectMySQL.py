import mysql.connector

def connect():
    mydb = mysql.connector.connect(
    host="localhost",
    port="3306",
    user="root",
    password="",
    #   database ="project"
    )
    mycursor = mydb.cursor()
    sql = """ CREATE DATABASE IF NOT EXISTS project;
            USE project; 
                create table IF NOT EXISTS user_info(
        userid varchar(50) NOT NULL ,
        birthdate Date ,
        profileLevel INT,
        gender INT,
        updatedTime DATETIME,
        PRIMARY KEY ( userid )
        );


  create table IF NOT EXISTS payment(
   userid varchar(50) NOT NULL,
   fistPaymentDate DATETIME,
   lastPaymentDate DATETIME,
   lastPaymentApp INT,
   PRIMARY KEY ( userid )
  );
  create table active(
   userId varchar(50) NOT NULL,
   fistActiveDate DATETIME,
   lastActiveDAte DATETIME,
   lastTransType INT,
   PRIMARY KEY ( userid )
  );

create table if NOT EXISTS userApp (
    userId varchar(50) NOT NULL,
    appId varchar(50) NOT NULL,
    PRIMARY KEY ( userId, appId )

  );

  create table if NOT EXISTS  userPmc (
    userId varchar(50) NOT NULL,
    pmcId varchar(50) NOT NULL,
    PRIMARY KEY ( userId, pmcId )

  );
  CREATE TABLE userPromotion (
    userid int,
    campaignID int,
    status varchar(15),
    Expire varchar(15),
    PRIMARY KEY (userid,campaignID)
);
"""
    mycursor.execute(sql)
    print("Connect sucsess !",mydb)
