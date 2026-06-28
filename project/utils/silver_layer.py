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

        return response

if __name__ == '__main__':
    obj=SilverLayer()
    obj.trigger_silver_layer('Retail_Sales_Silver_Layer','2026-06-28')
    