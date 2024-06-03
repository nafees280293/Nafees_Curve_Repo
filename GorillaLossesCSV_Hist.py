from datetime import datetime,timedelta
from airflow.utils.dates import days_ago
from airflow.decorators import task,dag
from MP2360Tools.MP2360Tools.utils import logz
from dateutil.relativedelta import relativedelta
from datetime import date
import boto3
import pyodbc
import json
import pandas as pd
import io
import csv

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
procedure = 'Sp_Get_Workflow_Details_to_create_DataComponent'
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

workflowname= 'ERCOT B2B MtM v4'
groupBy='losses'

args = {
    "owner": "Nafees",
    "start_date": days_ago(1),
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "on_failure":""
}

@dag(
    dag_id="Gorilla_Losses_CSV_Generation_Hist",
    schedule_interval=timedelta(1),
    tags=["RDS -> CSV -> S3"],
    default_args=args,
    catchup=False)


def generateCSVData():
    @task
    def insertData():
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        query = f"exec usp_getGorillaData 'GRP','"+workflowname+"','"+groupBy+"'"

        cursor.execute(query)
        rows = cursor.fetchall()

        groupData = []
        for row in rows:
            for i, col in enumerate(cursor.description):
                d = row[i]
            groupData.append(d)
        cursor.close()
        conn.close();

    
        for x in groupData:
            
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()

            if x=="UFE":
                query = f"exec sp_05_ERCOT_UFE_Hist"
                CSVNama = "05_UFE_Hist.csv"
            elif x=="TransmissionLossFactor":
                query = f"exec sp_04_ERCOT_Transmission_Losses_Hist"
                CSVNama = "04_Transmission_Loss_Hist.csv"
            else:
                query= f"exec sp_03_ERCOT_Distribution_Losses_Hist"
                CSVNama = "03_Distribution_Losses_Hist.csv"
                
            df = pd.read_sql_query(query, conn)

            with io.StringIO() as csv_buffer:
                df.to_csv(csv_buffer, index=False)

                response = s3.put_object(Bucket='curves-devenv', Key='Gorilla/'+CSVNama, Body=csv_buffer.getvalue())

                status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

                if status == 200:
                    print(f"Successful S3 put_object response. Status - {status}")
                else:
                    print(f"Unsuccessful S3 put_object response. Status - {status}")

            cursor.close()
            conn.close()

    
    insertData()
dag=generateCSVData()
