class BronzeLayer():

    def __init__(self):
        pass

    def ingest_data_api(self,url):
        import pandas as pd
        import requests
        from io import StringIO

        response=requests.get(url)
        if response.status_code==200:
            data=response.text

            df=pd.read_csv(StringIO(data))
            
            csvbuffer=StringIO()
            df.to_csv(csvbuffer,index=0)

            return csvbuffer.getvalue()
        else:
            print(f"Failed to fetch data. Status Code:{response.status_code}")

    def put_data_s3(self,bucketName,objectKey,data):

        import os
        from dotenv import load_dotenv
        import boto3

        load_dotenv()

        aws_access_key_id=os.getenv('aws_access_key_id')
        aws_secret_access_key=os.getenv('aws_secret_access_key')

        s3_client = boto3.client(
            's3',
            region_name='ap-south-2',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )

        s3_client.put_object(
            Bucket=bucketName,
            Key=objectKey,
            Body=data
        )

        return f"Data Loaded into S3 Bucket '{bucketName}',with object key '{objectKey}'"

if __name__=='__main__':
    obj=BronzeLayer()
    data=obj.ingest_data_api('https://raw.githubusercontent.com/HariHaran-2407/Retail-Orders-ETL-Pipeline-using-Airflow-and-AWS/refs/heads/main/dataset/customer.csv')
    obj.put_data_s3('airflowawsbuckethari','bronze/retail/customers.csv',data)
