from airflow.sdk import dag, task
from datetime import timedelta,datetime
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__),'..'))

from utils.bronze_layer import BronzeLayer
from utils.silver_layer import SilverLayer
from utils.gold_layer import GoldLayer


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
        date_folder = datetime.now().strftime("%Y-%m-%d")

        for url in urls:
            fetched_data=obj.ingest_data_api(url)
            obj.put_data_s3("airflowawsbuckethari",f"bronze/{date_folder}/{url.split('/')[-1]}",fetched_data)

        return date_folder
    
    @task(retries=3,retry_delay=timedelta(seconds=5))
    def trigger_silver_layer_spark_job(ti):
        last_load_date=ti.xcom_pull(task_ids='extract_load_s3',key='return_value')
        obj=SilverLayer()
        job_run_id = obj.trigger_silver_layer('Retail_Sales_Silver_Layer',last_load_date)
        print(f"Glue job triggered with JobRunId: {job_run_id}")

        return last_load_date
    
    @task
    def trigger_customer_crawler(ti):
        obj=SilverLayer()
        response = obj.trigger_crawler('customer_crawler')
        print(response)
    
    @task
    def trigger_orders_crawler(ti):
        obj=SilverLayer()
        response = obj.trigger_crawler('orders_crawler')
        print(response)
    
    @task
    def trigger_order_items_crawler(ti):
        obj=SilverLayer()
        response = obj.trigger_crawler('order_items_crawler')
        print(response)
    
    @task
    def trigger_products_crawler(ti):
        obj=SilverLayer()
        response = obj.trigger_crawler('products_crawler')
        print(response)
    
    @task(retries=3,retry_delay=timedelta(seconds=5))
    def trigger_databricks_job(ti):
        last_load_date=ti.xcom_pull(task_ids='trigger_silver_layer_spark_job',key='return_value')
        obj=GoldLayer()
        reponse = obj.trigger_databricks_job(401738252534212,last_load_date)
        print(reponse)

    
    extract_Bronze = extract_load_s3()
    transform_Silver = trigger_silver_layer_spark_job()

    customer_crawler=trigger_customer_crawler()
    orders_crawler=trigger_orders_crawler()
    order_items_crawler=trigger_order_items_crawler()
    products_crawler=trigger_products_crawler()
    databricks = trigger_databricks_job()

    extract_Bronze >> transform_Silver >> [customer_crawler,orders_crawler,order_items_crawler,products_crawler] >> databricks

retail_sales_project()
