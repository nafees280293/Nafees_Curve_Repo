from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.decorators import task,dag
from MP2360Tools.MP2360Tools.utils import logz
import boto3
import csv
import pyodbc
import json

logger = logz.create_logger()
#yest_date = datetime.now().date()+timedelta(days=-1)
#result = yest_date.strftime('%m%d%Y')
yest_date = datetime.now().date()+timedelta(days=-1)
result= yest_date.strftime('%m%d%Y')

s3 =  boto3.client('s3')
server = 'curves-dev.c3nmnzcia5f7.us-east-1.rds.amazonaws.com'
objC=s3.get_object(Bucket='curves-devenv', Key='config.txt')
dataC = objC['Body'].read().decode('utf-8').splitlines()
dataJson =  json.loads(dataC[0])

database = dataJson['database']
username = dataJson['username']
password = dataJson['password']
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

#yest_date = datetime.now().date()+timedelta(days=-1)
#result = yest_date.strftime('%m%d%Y')
args = {
    "owner": "Nafees",
    "start_date": days_ago(1),
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "on_failure":""
}

@dag(
    dag_id="Multiple_Insertion_Sena_Data",
    schedule_interval=timedelta(1),
    tags=["Pulling Sena data from S3 to RDS"],
    default_args=args,
    catchup=False)


def callInsertion():
    @task
    def insertLiborData():
        obj=s3.get_object(Bucket='curves-devenv', Key='CurvesData/Libor_'+str(result)+'.csv')

        data = obj['Body'].read().decode('utf-8').splitlines()
        records = csv.reader(data)
        headers = next(records)
        
        xml="<root>"
        for eachRecord in records:
            xml+="<row>"
            xml+="<SettleDate>"+eachRecord[0]+"</SettleDate>"
            xml+="<IrcCurveName>"+eachRecord[1]+"</IrcCurveName>"
            xml+="<CmContractMonth>"+eachRecord[2]+"</CmContractMonth>"
            xml+="<InterestRate>"+eachRecord[3]+"</InterestRate>"
            xml+="</row>"
        xml+="</root>"
    
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        query = f"exec usp_SenaInsertion '{xml}','lb'"
        cursor.execute(query)
        conn.commit()
        cursor.close()
        conn.close()
        

    @task
    def insertAncData():
        obj=s3.get_object(Bucket='curves-devenv', Key='CurvesData/Ancillary_'+str(result)+'.csv')

        data = obj['Body'].read().decode('utf-8').splitlines()
        records = csv.reader(data)
        headers = next(records)
        
        xml="<root>"
        for eachRecord in records:
            xml+="<row>"
            xml+="<SettlDate>"+eachRecord[0]+"</SettlDate>"
            xml+="<PpepPpPool>"+eachRecord[1]+"</PpepPpPool>"
            xml+="<PpepPepProduct>"+eachRecord[2]+"</PpepPepProduct>"
            xml+="<CmContractMonth>"+eachRecord[3]+"</CmContractMonth>"
            xml+="<Price>"+eachRecord[4]+"</Price>"
            xml+="</row>"
        xml+="</root>"
        
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        query = f"exec usp_SenaInsertion '{xml}','an'"
        cursor.execute(query)
        conn.commit()
        cursor.close()
        conn.close()

    @task
    def insertPjmData():
        obj = s3.get_object(Bucket='curves-devenv', Key='CurvesData/PJM_'+str(result)+'.csv')

        data = obj['Body'].read().decode('utf-8').splitlines()
        records = csv.reader(data)
        headers = next(records)

        xml = "<root>"
        for eachRecord in records:
            xml += "<row>"
            xml += "<Today>"+eachRecord[0]+"</Today>"
            xml += "<Region>"+eachRecord[1]+"</Region>"
            xml += "<PoolProduct>"+eachRecord[2]+"</PoolProduct>"
            xml += "<CmContractMonth>"+eachRecord[3]+"</CmContractMonth>"
            xml += "<Mp2Curve>"+eachRecord[4]+"</Mp2Curve>"
            xml += "<TodayPrice>"+eachRecord[5]+"</TodayPrice>"
            xml += "</row>"
        xml += "</root>"

        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        query = f"exec usp_SenaInsertion '{xml}','pj'"
        cursor.execute(query)
        conn.commit()
        cursor.close()
        conn.close()
	
    @task
    def insertGasData():
        obj = s3.get_object(Bucket='curves-devenv', Key='CurvesData/Gas_'+str(result)+'.csv')

        data = obj['Body'].read().decode('utf-8').splitlines()
        records = csv.reader(data)
        headers = next(records)

        xml = "<root>"
        for eachRecord in records:
            xml += "<row>"
            xml += "<SettleDate>"+eachRecord[0]+"</SettleDate>"
            xml += "<IndexCode>"+eachRecord[1]+"</IndexCode>"
            xml += "<ContractDate>"+eachRecord[2]+"</ContractDate>"
            #xml += "<CmContractMonth>"+eachRecord[3]+"</CmContractMonth>"
            xml += "<Price>"+eachRecord[3]+"</Price>"
            xml += "<Mp2Curve>"+eachRecord[4]+"</Mp2Curve>"
            xml += "</row>"
        xml += "</root>"

        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        query = f"exec usp_SenaInsertion '{xml}','gs'"
        cursor.execute(query)
        conn.commit()
        cursor.close()
        conn.close()

    @task
    def insertErcotData():
        obj = s3.get_object(Bucket='curves-devenv', Key='CurvesData/Ercot_'+str(result)+'.csv')

        data = obj['Body'].read().decode('utf-8').splitlines()
        records = csv.reader(data)
        headers = next(records)

        xml = "<root>"
        for eachRecord in records:
            xml += "<row>"
            xml += "<Today>"+eachRecord[0]+"</Today>"
            xml += "<Region>"+eachRecord[1]+"</Region>"
            xml += "<PoolProduct>"+eachRecord[2]+"</PoolProduct>"
            xml += "<CmContractMonth>"+eachRecord[3]+"</CmContractMonth>"
            xml += "<Mp2Curve>"+eachRecord[4]+"</Mp2Curve>"
            xml += "<TodayPrice>"+eachRecord[5]+"</TodayPrice>"
            xml += "</row>"
        xml += "</root>"

        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        query = f"exec usp_SenaInsertion '{xml}','er'"
        cursor.execute(query)
        conn.commit()
        cursor.close()
        conn.close()


    libor=insertLiborData() 
    anc=insertAncData()
    pjm=insertPjmData()
    gas=insertGasData()
    erc=insertErcotData()

dag=callInsertion()

