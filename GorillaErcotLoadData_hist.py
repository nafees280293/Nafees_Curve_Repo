from datetime import datetime,timedelta
from airflow.utils.dates import days_ago
from airflow.decorators import task,dag
from MP2360Tools.MP2360Tools.utils import logz
from dateutil.relativedelta import relativedelta
from datetime import date
import time
import requests
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
procedure = 'Sp_Get_Workflow_Details_to_create_DataComponent'
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

workflowname= 'ERCOT B2B MtM v4'
inputName = 'LoadDataWide'


args = {
    "owner": "Nafees",
    "start_date": days_ago(1),
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "on_failure":""
}

@dag(
    dag_id="Gorilla_Ercot_Load_Data",
    schedule_interval=timedelta(1), 
    tags=["Ercot Load Data by GoLang -> CSV -> Gorilla"],
    default_args=args,
    catchup=False)


def callInsertData():
    @task
    def insertData():
        csvName = 'LoadDataV'
        for x in range(6):
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()

            query = f"exec {procedure} '"+workflowname+"','"+inputName+"'"

            cursor.execute(query)
            workflowData = cursor.fetchall()

            cursor.close()
            conn.close();

            workflowid= workflowData[0][1]
            datatypeid=workflowData[0][7]
            CompName = workflowData[0][2]
            desc = workflowData[0][3]
            Gorilla_Workflow_Details_Id = str(workflowData[0][6])
            fileName = workflowData[0][4]
            
        
            if x!=0:
                jsonData ='{"data_component_id":"'+str(compId)+'","data_component_name":"'+CompName+'","data_component_description":"'+desc+'","file_name":"'+csvName+str(int(x+1))+'.csv","csv_file_name":"'+csvName+str(int(x+1))+'.csv"}'
            else:
                jsonData ='{"data_type_id":"'+datatypeid+'", "type": "file_cumulative","data_component_name":"'+CompName+'","data_component_description":"'+desc+'","file_name":"'+csvName+str(int(x+1))+'.csv","csv_file_name":"'+csvName+str(int(x+1))+'.csv"}'

            def get_access_token(url, client_id, client_secret):
                response = requests.post(
                    url,
                    data={"grant_type": "client_credentials","scope":f"{scope}"},
                    auth=(client_id, client_secret),
                )
                return response.json()["access_token"]


            token=get_access_token(token_url, client_id, client_secret)

            headers={'Authorization': f'Bearer {token}'}

            if x!=0:
                url = create_data_component_url+"create_data_component_version_file"
            else:
                url = create_data_component_url+"create_data_component_file"

            send_data = requests.post(url, data=jsonData , headers=headers)
    
            datasd = send_data.json()
    

            if send_data.status_code==200:
                dataCompId = datasd['data_component_id']
                compId =  datasd['data_component_id']
                version = datasd['version']
                dataCompStatus = datasd['status']

                conn = pyodbc.connect(conn_str)
                cursor = conn.cursor()
                query = f"exec Sp_Insert_DataComponent '"+CompName+"','"+desc+"','"+workflowid+"','"+dataCompId+"','"+version+"','"+dataCompStatus+"','"+Gorilla_Workflow_Details_Id+"',out"

                cursor.execute(query)
                conn.commit()

                cursor.close()
                conn.close()
                print("Order successfully sent")
            else:
                print(f"The following error was encountered. Error: {send_data.status_code}")
        
            time.sleep(60)


    insertData()

dag=callInsertData()


