from datetime import datetime, timedelta
import boto3
import csv
import pyodbc
import json

def create_logger():
    # Implement your logger creation logic here
    pass

logger = create_logger()

s3 = boto3.client('s3')
server = 'curves-dev.c3nmnzcia5f7.us-east-1.rds.amazonaws.com'
objC = s3.get_object(Bucket='curves-devenv', Key='config.txt')
dataC = objC['Body'].read().decode('utf-8').splitlines()
dataJson = json.loads(dataC[0])

database = dataJson['database']
username = dataJson['username']
password = dataJson['password']
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

obj = s3.get_object(Bucket='curves-devenv', Key='Forecast_Data/MP2_SITE_LEVEL_FORECAST_MONTHLY_02-25-2024.csv')
data = obj['Body'].read().decode('utf-8').splitlines()
records = csv.reader(data)
headers = next(records)

xml = "<root>"
for eachRecord in records:
    xml += "<row>"
    xml += "<Market>" + eachRecord[0] + "</Market>"
    xml += "<Utility>" + eachRecord[1] + "</Utility>"
    xml += "<Site>" + eachRecord[2] + "</Site>"
    xml += "<MonthNum>" + eachRecord[3] + "</MonthNum>"
    xml += "<YearNum>" + eachRecord[4] + "</YearNum>"
    xml += "<Product>" + eachRecord[5] + "</Product>"
    xml += "<Profile>" + eachRecord[6] + "</Profile>"
    xml += "<KwhLoadQty>" + eachRecord[7] + "</KwhLoadQty>"
    xml += "<RepOfRecord>" + eachRecord[8] + "</RepOfRecord>"
    xml += "<Lse>" + eachRecord[9] + "</Lse>"
    xml += "<LossType>" + eachRecord[10] + "</LossType>"
    xml += "<ContractNum>" + eachRecord[11] + "</ContractNum>"
    xml += "<KwhTotalLoadQty>" + eachRecord[12] + "</KwhTotalLoadQty>"
    xml += "<BlockLoadQty>" + eachRecord[13] + "</BlockLoadQty>"
    xml += "</row>"
xml += "</root>"

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()
query = f"exec usp_Gorilla_S3_Staging '{xml}','gl'"
cursor.execute(query)
conn.commit()
cursor.close()
conn.close()
