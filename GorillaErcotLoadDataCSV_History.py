from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.decorators import task,dag
from MP2360Tools.MP2360Tools.utils import logz
import io
import json
import boto3
import pyodbc
import json
import pandas as pd


logger = logz.create_logger()

s3 =  boto3.client('s3')

server = 'curves-dev.ckuiwkygw88o.us-east-1.rds.amazonaws.com'
objCDC=s3.get_object(Bucket='curves-devenv', Key='CostRevenue_config.txt')
dataCDC = objCDC['Body'].read().decode('utf-8').splitlines()
dataJson =  json.loads(dataCDC[0])

token_url = dataJson['token_url']
client_id = dataJson['client_id']
client_secret = dataJson['client_secret']
scope = dataJson['scope']
create_data_component_url = dataJson['create_data_component_url']


objC=s3.get_object(Bucket='curves-devenv', Key='config.txt')
dataC = objC['Body'].read().decode('utf-8').splitlines()
dataJson =  json.loads(dataC[0])


database = dataJson['database']
username = dataJson['username']
password = dataJson['password']
procedure = 'usp_Load_forcast_B2B_Hist'
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

query = f"exec {procedure} '0','0','counts'"

cursor.execute(query)
counters = cursor.fetchall()

cursor.close()
conn.close();

counters = counters[0][0]

args = {
    "owner": "Nafees",
    "start_date": days_ago(1),
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "on_failure":""
}

@dag(
    dag_id="Gorilla_Ercot_load_data_CSV_Generation",
    schedule_interval=timedelta(1),
    tags=["Get Data from RDS -> Create CSV -> Push CSV to S3"],
    default_args=args,
    catchup=False)


def callInsertData():
    @task
    def insertData():
        csvName = 'LoadDataV_Hist'
        firstCount = 0
        secondCount = counters
        for x in range(6):
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()

            query = f"exec {procedure} '"+str(firstCount)+"','"+str(firstCount+counters)+"','data'"

            df = pd.read_sql_query(query, conn)
        
            with io.StringIO() as csv_buffer:
                df.to_csv(csv_buffer, index=False)

                response = s3.put_object(Bucket='curves-devenv', Key='Gorilla/'+csvName+str(int(x+1))+'.csv', Body=csv_buffer.getvalue())

                status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

                if status == 200:
                    print(f"Successful S3 put_object response. Status - {status}")
                else:
                    print(f"Unsuccessful S3 put_object response. Status - {status}")

            cursor.close()
            conn.close()
                        
            firstCount=secondCount
            secondCount=firstCount+counters
    insertData()
dag=callInsertData()


