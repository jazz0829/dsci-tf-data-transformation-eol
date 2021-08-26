import boto3
from datetime import date
from pyspark.sql.types import *
from pyspark.sql.functions import col, trim, upper

def convertDataTypes(data_frame, integer_cols = [], long_cols = [], timestamp_cols = [], boolean_cols = [], double_cols = [], float_cols = [], date_cols=[]):
    for col_name in timestamp_cols:
        data_frame = data_frame.withColumn(col_name, col(col_name).cast(TimestampType()))

    for col_name in integer_cols:
        data_frame = data_frame.withColumn(col_name, col(col_name).cast(IntegerType()))

    for col_name in long_cols:
        data_frame = data_frame.withColumn(col_name, col(col_name).cast(LongType()))

    for col_name in boolean_cols:
        data_frame = data_frame.withColumn(col_name, col(col_name).cast(BooleanType()))

    for col_name in double_cols:
        data_frame = data_frame.withColumn(col_name, col(col_name).cast(DoubleType()))

    for col_name in float_cols:
        data_frame = data_frame.withColumn(col_name, col(col_name).cast(FloatType()))

    for col_name in date_cols:
        data_frame = data_frame.withColumn(col_name, col(col_name).cast(DateType()))

    return data_frame

def renameColumns(data_frame, mapping = {}):
    for source, destination in mapping.items():
        data_frame = data_frame.withColumnRenamed(source, destination)
    return data_frame

def cleanDataFrame(data_frame, to_upper_cols = []):
    for column_name in data_frame.columns:
        data_frame = data_frame.withColumn(column_name,trim(col(column_name)))
    for column_name in to_upper_cols:
        data_frame = data_frame.withColumn(column_name,upper(col(column_name)))
    return data_frame

def copy_parquets(s3BucketName, oldFolderKey, newFolderKey):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(s3BucketName)
    #delete old files
    for object in bucket.objects.filter(Prefix=newFolderKey):
        srcKey = object.key
        if not '_Temp/' in srcKey:
            s3.Object(s3BucketName, srcKey).delete()
    #move new files
    for object in bucket.objects.filter(Prefix=oldFolderKey):
        srcKey = object.key
        if not srcKey.endswith('/'):
            fileName = srcKey.split('/')[-1]
            destFileKey = newFolderKey + '/' + fileName
            copySource = s3BucketName + '/' + srcKey
            s3.Object(s3BucketName, destFileKey).copy_from(CopySource=copySource)
        s3.Object(s3BucketName, srcKey).delete()

def store_last_updated(table, item_name, updated):
    table.put_item(Item = {
        'Item': item_name,
        'LastUpdated':  int(updated)
    })

def get_last_updated(table, item_name):
    last_updated_response = table.get_item(Key={ 'Item': item_name })
    if ('Item' in last_updated_response):
        return date.fromtimestamp(last_updated_response['Item']['LastUpdated'])
    return None