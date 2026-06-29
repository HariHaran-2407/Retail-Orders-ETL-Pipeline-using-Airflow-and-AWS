import boto3
from datetime import timedelta, datetime
import time
import os
from dotenv import load_dotenv
load_dotenv()

class SilverLayer():

    def __init__(self):
        pass

    def trigger_silver_layer(self,job_name,parameter):

        aws_access_key_id=os.getenv("aws_access_key_id")
        aws_secret_access_key=os.getenv("aws_secret_access_key")
        glue_client=  boto3.client(
            'glue',
            region_name='ap-south-2',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )

        response = glue_client.start_job_run(
            JobName=job_name,
            Arguments={
                '--load_date':parameter
            }
        )

        job_run_id= response['JobRunId']
        while True:
            status = glue_client.get_job_run(
                JobName=job_name,
                RunId=job_run_id
            )['JobRun']['JobRunState']

            print(f"Current Status:{status}")

            if status in ['SUCCEEDED']:
                break
            elif status in ['FAILED','STOPPED']:
                raise Exception(f"Glue Job failed with status: {status}")
            
            time.sleep(30)

        print( f'{job_run_id}')


    #crawler 

    def trigger_crawler(self,crawler_name):

        aws_access_key_id=os.getenv('aws_access_key_id')
        aws_secret_access_key=os.getenv('aws_secret_access_key')

        glue_client=  boto3.client(
            'glue',
            region_name='ap-south-2',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )

        response = glue_client.start_crawler(
            Name=crawler_name
        )

        while True:
            status = glue_client.get_crawler(
                Name=crawler_name
            )['Crawler']['State']

            print(f"Current Status:{status}")

            if status in ['READY']:
                print("Crawler Completed Successfully")
                break
                
            time.sleep(30)

        print(response)



if __name__ == '__main__':
    obj=SilverLayer()
    # obj.trigger_silver_layer('Retail_Sales_Silver_Layer','2026-06-28')
    obj.trigger_crawler('customer_crawler')
    obj.trigger_crawler('orders_crawler')
    obj.trigger_crawler('order_items_crawler')


    