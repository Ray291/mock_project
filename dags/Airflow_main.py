from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from T1_check_File import *
from datetime import *
from connectMySQL import *
args={
	'owner': 'airflow',
    'email': ['createdforthesis@gmail.com','manhpd.esp@gmail.com','tuanminh7122001@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
}

def T1_check_File(**context):

    Date_now = context['execution_date']
    execution_Date = str(Date_now).split("T")[0].split("-")
    year = int(execution_Date[0])
    month = int(execution_Date[1])
    day = int(execution_Date[2])
    Store_Date = str(datetime(year, month, day) - timedelta(days=1)).split(" ")[0]
    print("DATE EXECUTION", Date_now)
    print("StoreDate in: ", Store_Date)
    status = check_statuts(Store_Date)
    info = str(Store_Date) + "|" + str(status[0]) + "|" + str(status[1]) + "|" + str(status[2])
    f = open("/home/nam/airflow/dags/data/info.txt", "w")
    f.write(info)
    f.close()


with DAG(
	dag_id = 'DEProject_Final',
	default_args = args,
	schedule_interval = timedelta(days = 1),
	start_date= datetime(year = 2021, month = 10, day = 31),
	dagrun_timeout = timedelta(minutes=60),
	catchup = True
) as dag:

    CheckAvailability = PythonOperator(
        task_id="CheckAvailability",
        provide_context=True,
        python_callable=T1_check_File,
        dag=dag
    )

    Spark = BashOperator(
        task_id="SparkSubmit",
        bash_command="spark-submit --master spark://Nam:7077 /home/nam/airflow/dags/spark_job.py",
        dag=dag
    )

    MySQLConnection = PythonOperator(
        task_id="MySQLConnector",
        python_callable=connect,
        dag=dag
    )

    send_email_notification = EmailOperator(
        task_id="send_email_notification",
        to=['createdforthesis@gmail.com','manhpd.esp@gmail.com','tuanminh7122001@gmail.com'],
        subject="DE4 Project Airflow Report",
        html_content=(  'Tasks run succesfully! xD <br>'
                        'Execute date: {{execution_date}}<br>'
                        'Log: <a href="{{ti.log_url}}">Link</a><br>'
                        'Host: {{ti.hostname}}<br>'
                        'Mark success: <a href="{{ti.mark_success_url}}">Link</a><br>'
                        )
    )

    CheckAvailability >> MySQLConnection >> Spark >> send_email_notification
