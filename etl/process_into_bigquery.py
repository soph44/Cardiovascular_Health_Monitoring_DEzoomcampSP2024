#Process data from datalake into BigQuery
#Remove unneccessary columns
#Remove rows with low confidence intervals

#Transform and Load into BigQuery
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.conf import SparkConf
from pyspark.sql.functions import spark_partition_id, regexp_replace
from pyspark.context import SparkContext
from google.cloud import storage
from google.cloud import bigquery as bq

#Input argument for which year of data to collect
parser = argparse.ArgumentParser()
parser.add_argument('--year', required=True)
args=parser.parse_args()
inputYear = args.year
inputYear = str(inputYear)

def check_bqtable_exists(client, table_id):
    try:
        client.get_table(table_id)
        return True
    except:
        return False

#Variables
inputYear = str(inputYear)
# keyLocation = '/Users/sonny/Git/de_zoomcamp_project_2024_SP/keys/cvd-key.json'
bucketName = 'cvd-bucket-de2024'

print(f'STARTING: Cleaning Data and Adding to BigQuery Table for {inputYear}')

#Create BigQuery Output External Table if not already existing, includes defined schema
client = bq.Client()
table_id = "cvd-sp-de-zoomcamp-2024.cvd_dataset.bq_output_table"
tableExists = check_bqtable_exists(client, table_id)
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
    dataset = bq.Dataset("cvd-sp-de-zoomcamp-2024.cvd_dataset")
    dataset.location = "US"
    dataset = client.create_dataset(dataset, timeout=30)
    new_table = bq.Table(table_id, schema=schema)
    new_table = client.create_table(new_table)
    print(f'BigQuery Table does not exist. Created.')

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
dfnew = spark.read \
    .option("header", "true") \
    .parquet('gs://cvd-bucket-de2024/cdc_data_' + inputYear + '.parquet') \
    .select(colRead)
print(f'Dataframe for {inputYear} read from GCS parquet file')

#Drop rows where any filtered column values are NULL
#Replace any "Georgia" or "Maryland" with "Georgia USA" and "Maryland USA" values under column "Locationdesc"
dfnew = dfnew.dropna(how='any')
dfnew = dfnew.withColumn("Locationdesc", regexp_replace("Locationdesc", "Georgia", "Georgia USA"))
dfnew = dfnew.withColumn("Locationdesc", regexp_replace("Locationdesc", "Maryland", "Maryland USA"))

#Pull existing bq output table and join unique values between the two
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

#Review partitions made
# dfpc.rdd.getNumPartitions()
# dfpc.withColumn("partitionId", spark_partition_id()).groupBy("partitionId").count().show()

#Write new table into bq, overwrite existing table
dfpc.write.format("bigquery")\
    .option("temporaryGcsBucket",bucketName)\
    .option("header", True) \
    .mode("overwrite")\
    .save(path=table_id)
    # .option("credentialsFile", keyLocation) \
print(f'Dataframe for {inputYear} joined and overwritten to BigQuery Output Table')

spark.stop()
print(f'COMPLETED: Data processing to BigQuery for {inputYear} completed. Spark stopped.')

# if __name__ == "__main__":
#     process_into_bigquery(2019)
#     process_into_bigquery(2020)
#     process_into_bigquery(2021)
#     process_into_bigquery(2022)
#     process_into_bigquery(2023)