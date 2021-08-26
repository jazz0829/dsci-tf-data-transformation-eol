import sys
import boto3
import json
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame

client = boto3.client('s3')
today = datetime.now()

year = today.strftime('%Y')
month = today.strftime('%m')
month_as_decimal = today.strftime('%-m')
day = today.strftime('%d')
day_as_decimal = today.strftime('%-d')

def path_exists(bucket_name, prefix):

    response = client.list_objects_v2(Bucket= bucket_name, Prefix= prefix)

    return response['KeyCount'] > 0

def get_environment_list(bucket_name, table_name, folder_name):
    path = "s3://{0}/environment={1}/{2}/{3}/{4}/{5}"
    s3_key_prefix = "environment={0}/{1}/{2}/{3}/{4}"
    path_list = []
    for env in ['NL','UK','BE','FR','US','ES','DE']:
        if folder_name is not None:
            prefix = s3_key_prefix.format(env,folder_name,year, month, day)
            if path_exists(bucket_name, prefix):
                path_list.append({'environment' : env, 'key_prefix' : path.format(bucket_name,env,folder_name,year, month, day)})
        else:
            prefix = s3_key_prefix.format(env,table_name,year, month, day)
            if path_exists(bucket_name, prefix):
                path_list.append({'environment' : env, 'key_prefix' : path.format(bucket_name,env,table_name,year, month, day)})

    return path_list

def get_dataframe(environment, s3_prefix):
    df = spark.read.parquet(s3_prefix)
    df = df.withColumn('environment',lit(environment))
    return df

def get_dataframe_schema(dataframe):
    return dataframe.schema

def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)

def process_table (table_name, folder_name, source_bucket, destination_bucket):
    files_to_copy = get_environment_list(source_bucket, table_name, folder_name)
    output_path = "s3://{0}/data/Source=EOLHosting/Object=HOST_CIG_{1}/IngestionYear={2}/IngestionMonth={3}/IngestionDay={4}".format(destination_bucket, table_name, year, month_as_decimal, day_as_decimal)
    dataframes_list = []
    for item in files_to_copy:
        dataframes_list.append(get_dataframe(environment = item['environment'], s3_prefix = item['key_prefix']))

    if len(dataframes_list) > 0 :
        result_df = unionAll(*dataframes_list)
        result_df = result_df.withColumn('cigcopytime', lit(str(today)))
        result_df = result_df.withColumn('cigprocessed',lit('False'))
        if 'timestamp' in result_df.columns:
            result_df = result_df.drop('timestamp')

        result_df.repartition(1).write.mode("overwrite").parquet(output_path)

if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ['source_bucket', 'destination_bucket','s3_artifact_bucket','configuration_file_key'])

    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    s3 = boto3.resource('s3')

    content_object = s3.Object(args['s3_artifact_bucket'], args['configuration_file_key'])
    file_content = content_object.get()['Body'].read().decode('utf-8')
    data = json.loads(file_content )

    for config in data['settings']:
        if config['is_enabled']:
            if 'folder_name' in config:
                process_table(config['table_name'], config['folder_name'], args['source_bucket'], args['destination_bucket'])
            else:
                process_table(config['table_name'], None, args['source_bucket'], args['destination_bucket'])