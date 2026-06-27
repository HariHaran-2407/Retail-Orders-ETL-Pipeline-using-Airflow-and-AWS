from airflow.sdk import dag, task
from datetime import timedelta,datetime
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__),'..'))

from utils.bronze_layer import BronzeLayer

date_folder = datetime.now().strftime("%Y-%m-%d")

@dag(
    schedule='@daily',
    is_paused_upon_creation=False
)
def retail_sales_project():

    @task(retries=3,retry_delay=timedelta(seconds=5))
    def extract_load_s3():
        urls=[
            "https://raw.githubusercontent.com/HariHaran-2407/Retail-Orders-ETL-Pipeline-using-Airflow-and-AWS/refs/heads/main/dataset/customer.csv",
            "https://raw.githubusercontent.com/HariHaran-2407/Retail-Orders-ETL-Pipeline-using-Airflow-and-AWS/refs/heads/main/dataset/order_items.csv",
            "https://raw.githubusercontent.com/HariHaran-2407/Retail-Orders-ETL-Pipeline-using-Airflow-and-AWS/refs/heads/main/dataset/products.csv",
            "https://raw.githubusercontent.com/HariHaran-2407/Retail-Orders-ETL-Pipeline-using-Airflow-and-AWS/refs/heads/main/dataset/orders.csv"
        ]
        obj=BronzeLayer()

        for url in urls:
            fetched_data=obj.ingest_data_api(url)
            obj.put_data_s3("airflowawsbuckethari",f"bronze/{date_folder}/{url.split('/')[-1]}",fetched_data)

        return date_folder
    
    extract_load_s3()
    
retail_sales_project()


