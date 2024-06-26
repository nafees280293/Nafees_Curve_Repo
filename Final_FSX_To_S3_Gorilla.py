from asyncio import tasks
from datetime import datetime, timedelta
from pathlib import Path
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.decorators import task,dag
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
from airflow.models import Variable
from MP2360Tools.MP2360Tools.utils import logz
import os


logger = logz.create_logger()

#folder_whitespace = "EOD Price Curves/"
#filepath=Path('/intel_data/ShellCurveFiles/' + folder_whitespace + 'Ercot_05162023.csv')

#current_date=datetime.date.today()+ datetime.timedelta(days=-1)
#result=current_date.strftime('%m%d%Y')

yest_date = datetime.now().date()
dateRef= yest_date.strftime('%m-%d-%Y')

filepath=Path('/Archive/MP2_SITE_LEVEL_FORECAST_MONTHLY_'+str(dateRef)+'.csv')
#filepath='/intel_data/ShellCurveFiles/EOD Price Curves/Ercot_05232023.csv'

#filepath=Path('/mnt/intel_data/ShellCurveFiles')
print(filepath)
Bucket="curves-devenv"

#dateRef=datetime.today().strftime('%Y-%m-%d')
#dateRef=datetime.today().strftime('%m%d%Y')
key='Forecast_Data/MP2_SITE_LEVEL_FORECAST_MONTHLY_' + str(dateRef) + '.csv'

args = {
    "owner": "Samson",
    "start_date": days_ago(1),
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "owner": "Nafees",
    "on_failure":""
}

@dag(
    dag_id="Final_FSX_TO_S3",
    schedule_interval=timedelta(1),
    tags=["Pulling data from function return and using it as args for another function"],
    default_args=args,
    catchup=False)




def curve_etl_Operations_Gorilla():
        
        @task(multiple_outputs=True)
        def file_read_from_drive_and_transform():

            if filepath.exists():
                #print(os.listdir(filepath))
                print(filepath)
                df=pd.read_csv(filepath)
                
                """Transform your dataframe to something you want to"""
                 
                processeddf=df.head(n=len(df))
                processeddf.to_csv(f'/tmp/MP2_SITE_LEVEL_FORECAST_MONTHLY_{dateRef}.csv',index=False)
                logger.info("Write temp location was successful")
                return {
                          "filepath":f'/tmp/MP2_SITE_LEVEL_FORECAST_MONTHLY_{dateRef}.csv'
                                    }
            else:
               raise Exception("File not found")
        
        @task
        def load_to_s3(filepath):
                if filepath !='' :
                    s3_hook = S3Hook(aws_conn_id="awss3Conn")
                    # Save processed data to CSV on S3
                    s3_hook.load_file(
                
                    filename=filepath,
                    bucket_name=Bucket,
                    key=key,
                    replace=True,
                    encrypt=True
                    )
                    return logger.info("Process successfully")
                else:  
                    raise Exception("File does not exist")
                
        getData=file_read_from_drive_and_transform()
        loadData=load_to_s3(filepath=getData["filepath"])

    
etl_dag=curve_etl_Operations_Gorilla()
