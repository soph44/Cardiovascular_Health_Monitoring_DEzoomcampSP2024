import argparse
import pandas as pd
import pyarrow
import wget
import os
import sys
import datetime
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import spark_partition_id, regexp_replace
from pyspark.context import SparkContext
from google.cloud import storage
from google.cloud import bigquery as bq


projectID = "cvd-sp-de-zoomcamp-2024" # <--- CHANGE TO YOUR PROJECT ID USED IN TERRAFORM
bucketName = 'cvd-bucket-de2024' # <--- CHANGE TO YOUR BUCKET NAME USED IN TERRAFORM

# Determines year to process into pipeline
# Note if data already exists for a specific year, the script will exit
# Script is set to run the year as [This Year 2024 - 2] for demonstration ecause 2024 data does not exist yet and 2023 has minimal data
today = datetime.date.today()
currentYear = today.year - 2 
#Input argument for which year of data to collect
parser = argparse.ArgumentParser()
parser.add_argument('--year', default=currentYear)
args=parser.parse_args()
inputYear = args.year

#Functions used to check if tables and datasets exists already
def check_bqtable_exists(client, table_id):
    try:
        client.get_table(table_id)
        return True
    except:
        return False

def check_bqdataset_exists(client, dataset_id):
    try:
        client.get_dataset(dataset_id)
        return True
    except:
        return False


#Variables
inputYear = str(inputYear)
filename = 'cdc_data_' + inputYear + '.csv'
filename_pq = 'cdc_data_' + inputYear + '.parquet'
pathname_pq = 'gs://' + bucketName + '/' + filename_pq
dataset_id = projectID + ".cvd_dataset"
table_id = projectID + ".cvd_dataset.bq_output_table"
datasetName = projectID +".cvd_dataset"

#Check that data doesn't already exist for the inputYear
#If it already exists, exit pipeline
storage_client = storage.Client() #.from_service_account_json(keyLocation)
bucket = storage_client.bucket(bucketName)
pqExists = storage.Blob(bucket=bucket, name=pathname_pq).exists(storage_client)
if pqExists == True:
    print(f'Parquet data for {inputYear} already exists in GCS Bucket. Exiting run...')
    sys.exit(0)

#---Download CDC Files to GCS---
#Download csv files locally and convert into parquet files with enforced schema (dtypes)
#Dump parquet data into GCS datalake
print(f'\nSTARTING: Downloading CDC CVD Data for {inputYear} to GCS')

#Create local directory for downloaded csv from CDC site
if not os.path.isdir('./data'):
    os.mkdir('./data')
if not os.path.isdir('./data/pq'):
    os.mkdir('./data/pq')

URL = "https://data.cdc.gov/api/views/dttw-5yxu/rows.csv?query=select%20*%20where%20((%60year%60%20%3D%20%27" \
    + inputYear + \
    "%27)%20AND%20%60year%60%20IS%20NOT%20NULL)%20AND%20(%60topic%60%20%3D%20%27Cardiovascular%20Disease%27)&read_from_nbe=true&version=2.1&date=20240407&accessType=DOWNLOAD"
print('URL = ', URL)

if os.path.exists(f'./data/pq/{filename_pq}'):
    os.remove(f'./data/pq/{filename_pq}')
if os.path.exists(f'./data/{filename}'):
    os.remove(f'./data/{filename}')

response = wget.download(URL, './data/' + filename)

#Note that Pandas was used to enforce the schema. The limitation of Pandas is that there is no datetime dtype, but in this dataset, datetime is not used.
#Note that Pandas uses 'object' as the dtype for strings
schema = {
    "Year" : 'int',
    "Locationabbr" : 'str',
    "Locationdesc" : 'str',
    "Class" : 'str',
    "Topic" : 'str',
    "Question" : 'str',
    "Response" : 'str',
    "Break_Out" : 'str',
    "Break_Out_Category" : 'str',
    "Sample_Size" : 'int',
    "Data_value" : 'float',
    "Confidence_limit_Low" : 'float',
    "Confidence_limit_High" : 'float',
    "Display_order" : 'int',
    "Data_value_unit" : 'str',
    "Data_value_type" : 'str',
    "Data_Value_Footnote_Symbol" : 'str',
    "Data_Value_Footnote" : 'str',
    "DataSource" : 'str',
    "ClassId" : 'str',
    "TopicId" : 'str',
    "LocationID" : 'int',
    "BreakoutID" : 'str',
    "BreakOutCategoryID" : 'str',
    "QuestionID" : 'str',
    "ResponseID" : 'str',
    "GeoLocation" : 'str'
}

#Read csv data into pandas dataframe
df = pd.read_csv(f'./data/{filename}', dtype=schema)
#Create unique key based off other column values
df['Key'] = df['Year'].astype(str)+df['Locationabbr'].astype(str)+df['QuestionID'].astype(str)+\
        df['ResponseID'].astype(str)+df['BreakOutCategoryID'].astype(str)+\
        df['BreakoutID'].astype(str)
df.set_index('Key', inplace=True)
#Save to local/VM parquet
df.to_parquet(f'./data/pq/{filename_pq}')

#Check that Bucket exists then Upload to GCS bucket
#Create bucket prior to upload if not existing
buckets = storage_client.list_buckets()

bucketExist = False
for bucket in buckets:
    if bucket.name == bucketName:
        bucketExist = True
        break

if bucketExist == False:
    bucket = storage_client.create_bucket(bucketName)
    print(f'GCS bucket does not exist. Created')
else:
    bucket = storage_client.bucket(bucketName)

blob = bucket.blob(filename_pq)
blob.upload_from_filename(f'./data/pq/{filename_pq}')

#Remove local files
os.remove(f'./data/{filename}')
os.remove(f'./data/pq/{filename_pq}')
print(f'COMPLETED: Downloaded CDC CVD Data for {inputYear} to GCS')


#---Process into BigQuery and Apply Transformations---
print(f'STARTING: Cleaning Data and Adding to BigQuery Table for {inputYear}')

#Create BigQuery Output External Table if not already existing, includes defined schema
client = bq.Client()
tableExists = check_bqtable_exists(client, table_id)
datasetExists = check_bqdataset_exists(client, dataset_id)
if datasetExists == False:
    dataset = bq.Dataset(datasetName)
    dataset.location = "US"
    dataset = client.create_dataset(dataset, timeout=30)
    print(f'BigQuery Dataset does not exist. Created.')
if tableExists == False:
    schema = [
        bq.SchemaField("Key", "STRING"),
        bq.SchemaField("Year", "INTEGER"),
        bq.SchemaField("Locationdesc", "STRING"),
        bq.SchemaField("Question", "STRING"),
        bq.SchemaField("Response", "STRING"),
        bq.SchemaField("Break_Out", "STRING"),
        bq.SchemaField("Break_Out_Category", "STRING"),
        bq.SchemaField("Sample_Size", "INTEGER"),
        bq.SchemaField("Data_value", "FLOAT"),
        bq.SchemaField("QuestionID", "STRING"),
    ]
    new_table = bq.Table(table_id, schema=schema)
    new_table = client.create_table(new_table)
    print(f'BigQuery Table does not exist. Created.')

# ## START Configuration for running local Spark instance
# keyLocation = './keys/cvd-key.json'
# conf = SparkConf() \
#     .setMaster('local[*]') \
#     .setAppName('cvdPipeline') \
#     .set("spark.jars", "./lib/gcs-connector-hadoop3-2.2.5.jar,./lib/spark-bigquery-with-dependencies_2.12-0.37.0.jar") \
#     .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
#     .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", keyLocation)
# print(f'Spark configuration Set.')

# print(f'Setting Hadoop Configuration.')
# sc = SparkContext(conf=conf)
# hadoop_conf = sc._jsc.hadoopConfiguration()
# hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
# hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
# hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", keyLocation)
# hadoop_conf.set("fs.gs.auth.service.account.enable", "true")
# print(f'Hadoop configuration set.')
# ## END Configuration for running local Spark instance

#Start Spark instance
print(f'Starting SparkSession.')
spark = SparkSession.builder \
    .appName('cvdPipeline') \
    .getOrCreate()
    # .config(conf=sc.getConf()) \
spark.conf.set('temporaryGcsBucket', bucketName)
print(f'SparkSession created.')

#Read added year and clean data for redundant/unused columns. Drop rows of low confidence data
colRead = ['Key', 'Year', 'Locationdesc', 'Question', \
            'Response', 'Break_Out', 'Break_Out_Category', \
            'Sample_Size', 'Data_value', 'QuestionID'
            ]

#Read newly added CDC data from GCS bucket
dfnew = spark.read \
    .option("header", "true") \
    .parquet(pathname_pq) \
    .select(colRead)
print(f'Dataframe for {inputYear} read from GCS parquet file')

#Drop rows where any filtered column values are NULL
#Replace any "Georgia" or "Maryland" with "Georgia USA" and "Maryland USA" values under column "Locationdesc"
dfnew = dfnew.dropna(how='any')
dfnew = dfnew.withColumn("Locationdesc", regexp_replace("Locationdesc", "Georgia", "Georgia USA"))
dfnew = dfnew.withColumn("Locationdesc", regexp_replace("Locationdesc", "Maryland", "Maryland USA"))

#Pull existing BigQuery output table and join unique values between the two
dfbq = spark.read.format('bigquery') \
    .option('table', table_id) \
    .option("header", True) \
    .load()
    # .option("credentialsFile", keyLocation) \
print(f'Dataframe read from existing BigQuery Output Table')

#Join dataframes
dfjoin = dfbq.union(dfnew)

#Set number of partitions and cluster by Break_Out_Category
dfpc = dfjoin.repartition(6, "Break_Out_Category")

#Write new table into bq, overwrite existing table
dfpc.write.format("bigquery")\
    .option("temporaryGcsBucket",bucketName)\
    .option("header", True) \
    .mode("overwrite")\
    .save(path=table_id)
    # .option("credentialsFile", keyLocation) \
print(f'Dataframe for {inputYear} joined and overwritten to BigQuery Output Table')

