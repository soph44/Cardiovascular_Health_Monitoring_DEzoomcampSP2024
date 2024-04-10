import argparse
import pandas as pd
import pyarrow
import wget
import os
import shutil
from google.cloud import storage

#Input argument for which year of data to collect
parser = argparse.ArgumentParser()
parser.add_argument('--year', required=True)
args=parser.parse_args()
inputYear = args.year
inputYear = str(inputYear)

#Download csv files locally and convert into parquet files with enforced schema (dtypes)
#Dump parquet data into GCS datalake
# def download_to_datalake_GCS(inputYear):

#Variables
# inputYear = str(inputYear)
# keyLocation = './keys/cvd-key.json'
bucketName = 'cvd-bucket-de2024'
filename = 'cdc_data_' + inputYear + '.csv'
filename_pq = 'cdc_data_' + inputYear + '.parquet'

print(f'STARTING: Downloading CDC CVD Data for {inputYear} to GCS')

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

'''
Note that Pandas was used to enforce the schema. The limitation of Pandas is that there is no datetime dtype, but in this dataset, datetime is not used.
Note that Pandas uses 'object' as the dtype for strings
'''

df = pd.read_csv(f'./data/{filename}', dtype=schema)
df['Key'] = df['Year'].astype(str)+df['Locationabbr'].astype(str)+df['QuestionID'].astype(str)+\
        df['ResponseID'].astype(str)+df['BreakOutCategoryID'].astype(str)+\
        df['BreakoutID'].astype(str)
df.set_index('Key', inplace=True)
df.to_parquet(f'./data/pq/{filename_pq}')

#Check that Bucket exists then Upload to GCS bucket
storage_client = storage.Client #.from_service_account_json(keyLocation) 
buckets = storage_client.list_buckets()
bucketExist = False
for bucket in buckets:
    if bucket.name == bucketName:
        bucketExist = True
if bucketExist == False:
    bucket = storage_client.create_bucket(bucketName)
    print(f'GCS bucket does not exist. Created')
else:
    bucket = storage_client.bucket(bucketName)
blob = bucket.blob(filename_pq)
blob.upload_from_filename(f'./data/pq/{filename_pq}')
print(f'COMPLETED: Downloaded CDC CVD Data for {inputYear} to GCS')

shutil.rmtree('./data/')
shutil.rmtree('./data/pq/')
# if __name__ == "__main__":
#     download_to_datalake_GCS(2019)
#     download_to_datalake_GCS(2020)
#     download_to_datalake_GCS(2021)
#     download_to_datalake_GCS(2022)
#     download_to_datalake_GCS(2023)




