Data Engineering Project

A data processing system for this purpose, from extracting, loading and transforming data into understandable database tables is built in the scope of the project. The project inquiries users profile, daily transactions and promotions data from a payment application. 

Prerequisites

spark-3.2.0-bin-hadoop3.2
apache-airflow-2.2.4
openjdk 11.0.13
Python 3.8.10

Installation
Airflow 

pip install "apache-airflow[celery]==2.2.4" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.2.4/constraints-3.8.txt"
airflow db init
airflow webserver --port 8080
airflow scheduler

Achieve Airflow UI at http://localhost:8080

MySQL

git clone https://github.com/bitnami/bitnami-docker-mysql.git
cd bitnami-docker-mysql
docker-compose up -d

Spark

Follow https://phoenixnap.com/kb/install-spark-on-ubuntu to install spark and start master and workers nodes

Running
On Airflow UI, select DEProject_Final DAG
Choose option Trigger DAG to run pipeline
