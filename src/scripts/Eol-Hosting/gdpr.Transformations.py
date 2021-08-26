import sys
from utils import get_last_updated, store_last_updated
from datetime import date, timedelta
import time

import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, when, lit
from pyspark.sql.types import TimestampType, StringType

athena_output_location = None
athena_client = None
s3_client = None
s3_client_ll = None
spark = SparkSession.builder.getOrCreate()
glueContext = GlueContext(SparkContext.getOrCreate())


def query_athena(query, max_retry=30):
    global athena_output_location, athena_client

    execution = athena_client.start_query_execution(
        QueryString=query,
        ResultConfiguration={
            'OutputLocation': athena_output_location
        }
    )
    execution_id = execution['QueryExecutionId']
    state = 'RUNNING'

    while (max_retry > 0 and state in ['RUNNING']):
        max_retry = max_retry - 1
        response = athena_client.get_query_execution(QueryExecutionId=execution_id)

        if 'QueryExecution' in response and 'Status' in response['QueryExecution'] and 'State' in \
                response['QueryExecution']['Status']:
            state = response['QueryExecution']['Status']['State']
            if state == 'FAILED':
                return False
            elif state == 'SUCCEEDED':
                return execution_id
        time.sleep(1)

    return False


def format_result(results):
    values = []
    data_list = []
    for row in results['ResultSet']['Rows']:
        data_list.append(row['Data'])
    for datum in data_list[1:]:
        try:
            values.extend([x['VarCharValue'] for x in datum])
        except:
            pass
    return values


def cleanup(bucket, prefix):
    global s3_client
    s3_bucket = s3_client.Bucket(bucket)
    for item in s3_bucket.objects.filter(Prefix=prefix):
        item.delete()


def get_list_by_query(query, max_retry=30):
    global athena_client, athena_output_location
    query_id = query_athena(query, max_retry)
    if query_id == False:
        raise Exception('Query cannot be done with ' + str(max_retry) + ' retries')
    marker = None
    i = 0
    result = []
    while True:
        paginator = athena_client.get_paginator('get_query_results')
        response_iterator = paginator.paginate(
            QueryExecutionId=query_id,
            PaginationConfig={
                'MaxItems': 1000,
                'PageSize': 1000,
                'StartingToken': marker})

        for page in response_iterator:
            i = i + 1
            values = format_result(page)
            result.extend(values)
        try:
            marker = page['NextToken']
        except KeyError:
            break
    return result


def delete_raw_accounts(lastupdated_table, raw_bucket):
    # DELETES HISTORY OF ACCOUNTS TABLE RECORDS WHERE THE ACCOUNT ISANONYMIZED, LEAVING ONLY THE LATEST ANONYMIZED RECORD
    global spark, s3_client, s3_client_ll

    last_updated = get_last_updated(lastupdated_table, 'GDPR_Delete_Raw_Accounts')

    today = date.today()
    difference = (today - last_updated).days

    i = 1
    while i <= difference:
        current_date = last_updated + timedelta(days=i)
        year = str(current_date.year)
        month = str(current_date.month)
        day = str(current_date.day)
        query = 'SELECT accountid FROM "customerintelligence"."config_gdpr_accountsdeletionlog" where year = \'' + year + '\' and month = \'' + month + '\' and day = \'' + day + '\''
        account_ids = get_list_by_query(query)
        query = 'SELECT distinct(path) FROM (SELECT id, "$path" as path, ROW_NUMBER() OVER (PARTITION BY id ORDER BY cigcopytime DESC) AS row_num ' + \
                'FROM "customerintelligence_raw"."object_host_cig_accounts") AS t WHERE row_num > 1 and id IN ' + \
                '(' + (','.join(list(map(lambda x: '\'' + str(x) + '\'', account_ids)))) + ')'
        if len(account_ids) > 0:
            file_paths = get_list_by_query(query)
            window_spec = Window.partitionBy(col('id')).orderBy(col('cigcopytime').desc())
            for file_path in file_paths:
                print(file_path)
                accounts_raw_df = spark.read.parquet(file_path)
                accounts_raw_df = accounts_raw_df.withColumn('cigcopytime', col('cigcopytime').cast(TimestampType()))
                accounts_raw_df = accounts_raw_df.withColumn("row_num", row_number().over(window_spec))
                accounts_raw_df = accounts_raw_df.where(
                    (col("row_num") == 1) | ((col("row_num") > 1) & (col('id').isin(account_ids) == False)))
                accounts_raw_df = accounts_raw_df.drop("row_num")
                accounts_raw_df = accounts_raw_df.withColumn('cigcopytime', col('cigcopytime').cast(StringType()))
                accounts_raw_df.repartition(1).write.mode("overwrite").parquet(file_path + '_temp')

                s3_path = file_path + '_temp'
                dir_key = s3_path.replace("s3://", "").split("/",1)[-1]

                find_obj = s3_client_ll.list_objects(Bucket=raw_bucket, Prefix=dir_key)
                file_key = \
                [something for something in find_obj['Contents'] if (something['Key'].split('.')[-1] == 'parquet')][0][
                    'Key']
                print("Lookhere",file_path,file_key,dir_key)
                s3_client.Object(raw_bucket, file_path).copy_from(CopySource=raw_bucket+'/'+file_key)
                #s3_client.Object(raw_bucket, file_path + '_temp').delete()
                bucket = s3_client.Bucket(raw_bucket)
                bucket.objects.filter(Prefix=dir_key).delete()
        store_last_updated(lastupdated_table, 'GDPR_Delete_Raw_Accounts', time.mktime(current_date.timetuple()))
        i = i + 1


def anonymize_divisions(lastupdated_table, raw_bucket):
    # ANOMYNIZE DIVISIONS TABLE OF ALL DIVISIONS BELONGING TO AN ACCOUNT THAT HAS BEEN ANONYMIZED
    # We keep the history, rather than delete so that we can keep a history of when a particular division became active or moved between accounts (e.g. when moved to an accountant)
    global spark, s3_client

    last_updated = get_last_updated(lastupdated_table, 'GDPR_Anonymize_Raw_Divisions')

    today = date.today()
    difference = (today - last_updated).days

    anonymized_value = lit('####')

    i = 1
    while i <= difference:
        current_date = last_updated + timedelta(days=i)
        year = str(current_date.year)
        month = str(current_date.month)
        day = str(current_date.day)

        query = 'SELECT accountid FROM "customerintelligence"."config_gdpr_accountsdeletionlog" where "year" = \'' + year + '\' and "month" = \'' + month + '\' and "day" = \'' + day + '\''
        account_ids = get_list_by_query(query)

        query = 'SELECT distinct(path) FROM (SELECT "$path" as path FROM "customerintelligence_raw"."object_host_cig_divisions" WHERE customer IN ' + \
                '(' + (','.join(list(map(lambda x: '\'' + str(x) + '\'', account_ids)))) + ')'
        if len(account_ids) > 0:
            file_paths = get_list_by_query(query)
            for file_path in file_paths:
                divisions_raw_df = spark.read.parquet(file_path)
                divisions_raw_df = divisions_raw_df.withColumn('description', when(col('customer').isin(account_ids),
                                                                                   anonymized_value).otherwise(
                    col('description')))
                divisions_raw_df = divisions_raw_df.withColumn('chamberofcommerce',
                                                               when(col('customer').isin(account_ids),
                                                                    anonymized_value).otherwise(
                                                                   col('chamberofcommerce')))
                divisions_raw_df = divisions_raw_df.withColumn('city', when(col('customer').isin(account_ids),
                                                                            anonymized_value).otherwise(col('city')))
                divisions_raw_df = divisions_raw_df.withColumn('postcode', when(col('customer').isin(account_ids),
                                                                                anonymized_value).otherwise(
                    col('postcode')))
                divisions_raw_df = divisions_raw_df.withColumn('dunsnumber', when(col('customer').isin(account_ids),
                                                                                  anonymized_value).otherwise(
                    col('dunsnumber')))
                divisions_raw_df = divisions_raw_df.withColumn('website', when(col('customer').isin(account_ids),
                                                                               anonymized_value).otherwise(
                    col('website')))
                divisions_raw_df.repartition(1).write.mode("overwrite").parquet(file_path + '_temp')
                s3_client.Object(raw_bucket, file_path).copy_from(CopySource=file_path + '_temp')
                s3_client.Object(raw_bucket, file_path + '_temp').delete()

        store_last_updated(lastupdated_table, 'GDPR_Anonymize_Raw_Divisions', time.mktime(current_date.timetuple()))
        i = i + 1


def delete_salesforce_accounts(lastupdated_table, raw_bucket):
    # ANOMYNIZE DIVISIONS TABLE OF ALL DIVISIONS BELONGING TO AN ACCOUNT THAT HAS BEEN ANONYMIZED
    # We keep the history, rather than delete so that we can keep a history of when a particular division became active or moved between accounts (e.g. when moved to an accountant)
    global spark, s3_client

    last_updated = get_last_updated(lastupdated_table, 'GDPR_Delete_Raw_Salesforce_Accounts')

    today = date.today()
    difference = (today - last_updated).days

    i = 1
    while i <= difference:
        current_date = last_updated + timedelta(days=i)
        year = str(current_date.year)
        month = str(current_date.month)
        day = str(current_date.day)

        query = 'SELECT accountid FROM "customerintelligence"."config_gdpr_accountsdeletionlog" where "year" = \'' + year + '\' and "month" = \'' + month + '\' and "day" = \'' + day + '\''
        account_ids = get_list_by_query(query)

        query = 'SELECT distinct(path) FROM (SELECT "$path" as path FROM "customerintelligence_raw"."object_account" WHERE exact_id__c IN ' + \
                '(' + (','.join(list(map(lambda x: '\'' + str(x) + '\'', account_ids)))) + ')'

        if len(account_ids) > 0:
            file_paths = get_list_by_query(query)
            window_spec = Window.partitionBy(col('id')).orderBy(col('etlinserttime').desc(), col('lastmodifieddate').desc())
            for file_path in file_paths:
                accounts_raw_df = spark.read.parquet(file_path)
                accounts_raw_df = accounts_raw_df.withColumn('etlinserttime',
                                                             col('etlinserttime').cast(TimestampType()))
                accounts_raw_df = accounts_raw_df.withColumn('lastmodifieddate',
                                                             col('lastmodifieddate').cast(TimestampType()))
                accounts_raw_df = accounts_raw_df.withColumn("row_num", row_number().over(window_spec))
                accounts_raw_df = accounts_raw_df.where(
                    (col("row_num") == 1) | ((col("row_num") > 1) & (col('exact_id__c').isin(account_ids) == False)))
                accounts_raw_df = accounts_raw_df.drop("row_num")
                accounts_raw_df.repartition(1).write.mode("overwrite").parquet(file_path + '_temp')
                s3_client.Object(raw_bucket, file_path).copy_from(CopySource=file_path + '_temp')
                s3_client.Object(raw_bucket, file_path + '_temp').delete()

        store_last_updated(lastupdated_table, 'GDPR_Delete_Raw_Salesforce_Accounts',
                           time.mktime(current_date.timetuple()))
        i = i + 1


def delete_raw_persons(lastupdated_table, raw_bucket):
    # DELETES HISTORY OF ACCOUNTS TABLE RECORDS WHERE THE ACCOUNT ISANONYMIZED, LEAVING ONLY THE LATEST ANONYMIZED RECORD
    global spark, s3_client

    last_updated = get_last_updated(lastupdated_table, 'GDPR_Delete_Raw_Persons')

    today = date.today()
    difference = (today - last_updated).days

    i = 1
    while i <= difference:
        current_date = last_updated + timedelta(days=i)
        year = str(current_date.year)
        month = str(current_date.month)
        day = str(current_date.day)

        query = 'SELECT personid FROM "customerintelligence"."config_gdpr_personsdeletionlog" where "year" = \'' + year + '\' and "month" = \'' + month + '\' and "day" = \'' + day + '\''
        account_ids = get_list_by_query(query)

        query = 'SELECT distinct(path) FROM (SELECT id, "$path" as path, ROW_NUMBER() OVER (PARTITION BY id ORDER BY cigcopytime DESC) AS row_num ' + \
                'FROM "customerintelligence_raw"."object_host_cig_persons") AS t WHERE row_num > 1 and id IN ' + \
                '(' + (','.join(list(map(lambda x: '\'' + str(x) + '\'', account_ids)))) + '))'
        if len(account_ids) > 0:
            file_paths = get_list_by_query(query)
            window_spec = Window.partitionBy(col('id')).orderBy(col('cigcopytime').desc())
            for file_path in file_paths:
                persons_raw_df = spark.read.parquet(file_path)
                persons_raw_df = persons_raw_df.withColumn('cigcopytime', col('cigcopytime').cast(TimestampType()))
                persons_raw_df = persons_raw_df.withColumn("row_num", row_number().over(window_spec))
                persons_raw_df = persons_raw_df.where(
                    (col("row_num") == 1) | ((col("row_num") > 1) & (col('id').isin(account_ids) == False)))
                persons_raw_df = persons_raw_df.drop("row_num")
                persons_raw_df.repartition(1).write.mode("overwrite").parquet(file_path + '_temp')
                s3_client.Object(raw_bucket, file_path).copy_from(CopySource=file_path + '_temp')
                s3_client.Object(raw_bucket, file_path + '_temp').delete()

        store_last_updated(lastupdated_table, 'GDPR_Delete_Raw_Persons', time.mktime(current_date.timetuple()))
        i = i + 1


def delete_raw_users(lastupdated_table, raw_bucket):
    # DELETES HISTORY OF ACCOUNTS TABLE RECORDS WHERE THE ACCOUNT ISANONYMIZED, LEAVING ONLY THE LATEST ANONYMIZED RECORD
    global spark, s3_client

    last_updated = get_last_updated(lastupdated_table, 'GDPR_Delete_Raw_Users')

    today = date.today()
    difference = (today - last_updated).days

    i = 1
    while i <= difference:
        current_date = last_updated + timedelta(days=i)
        year = str(current_date.year)
        month = str(current_date.month)
        day = str(current_date.day)

        query = 'SELECT personid FROM "customerintelligence"."config_gdpr_personsdeletionlog" where "year" = \'' + year + '\' and "month" = \'' + month + '\' and "day" = \'' + day + '\''
        account_ids = get_list_by_query(query)

        if len(account_ids) > 0:
            query = 'SELECT distinct(path) FROM (SELECT id, "$path" as path, ROW_NUMBER() OVER (PARTITION BY id ORDER BY cigcopytime DESC) AS row_num ' + \
                    'FROM "customerintelligence_raw"."object_host_cig_users") AS t WHERE row_num > 1 and person IN ' + \
                    '(' + (','.join(list(map(lambda x: '\'' + str(x) + '\'', account_ids)))) + '))'
            file_paths = get_list_by_query(query)
            window_spec = Window.partitionBy(col('id')).orderBy(col('cigcopytime').desc())
            for file_path in file_paths:
                persons_raw_df = spark.read.parquet(file_path)
                persons_raw_df = persons_raw_df.withColumn('cigcopytime', col('cigcopytime').cast(TimestampType()))
                persons_raw_df = persons_raw_df.withColumn("row_num", row_number().over(window_spec))
                persons_raw_df = persons_raw_df.where(
                    (col("row_num") == 1) | ((col("row_num") > 1) & (col('id').isin(account_ids) == False)))
                persons_raw_df = persons_raw_df.drop("row_num")
                persons_raw_df.repartition(1).write.mode("overwrite").parquet(file_path + '_temp')
                s3_client.Object(raw_bucket, file_path).copy_from(CopySource=file_path + '_temp')
                s3_client.Object(raw_bucket, file_path + '_temp').delete()

        store_last_updated(lastupdated_table, 'GDPR_Delete_Raw_Users', time.mktime(current_date.timetuple()))
        i = i + 1


def delete_raw_salesforce_users(lastupdated_table, raw_bucket):
    # DELETES HISTORY OF ACCOUNTS TABLE RECORDS WHERE THE ACCOUNT ISANONYMIZED, LEAVING ONLY THE LATEST ANONYMIZED RECORD
    global spark, s3_client

    last_updated = get_last_updated(lastupdated_table, 'GDPR_Delete_Raw_Salesforce_Users')

    today = date.today()
    difference = (today - last_updated).days

    i = 1
    while i <= difference:
        current_date = last_updated + timedelta(days=i)
        year = str(current_date.year)
        month = str(current_date.month)
        day = str(current_date.day)

        query = 'SELECT userid FROM "customerintelligence"."config_gdpr_personsdeletionlog" A join "customerintelligence"."users" B on A.personid = b.personid  ' + \
                'where "year" = \'' + year + '\' and "month" = \'' + month + '\' and "day" = \'' + day + '\''
        account_ids = get_list_by_query(query)

        if len(account_ids) > 0:
            query = 'SELECT distinct(path) FROM (SELECT id, "$path" as path, ROW_NUMBER() OVER (PARTITION BY id ORDER BY cigcopytime DESC) AS row_num ' + \
                    'FROM "customerintelligence_raw"."object_user") AS t WHERE row_num > 1 and userid__c IN ' + \
                    '(' + (','.join(list(map(lambda x: '\'' + str(x) + '\'', account_ids)))) + '))'
            file_paths = get_list_by_query(query)
            window_spec = Window.partitionBy(col('id')).orderBy(col('cigcopytime').desc())
            for file_path in file_paths:
                persons_raw_df = spark.read.parquet(file_path)
                persons_raw_df = persons_raw_df.withColumn('cigcopytime', col('cigcopytime').cast(TimestampType()))
                persons_raw_df = persons_raw_df.withColumn("row_num", row_number().over(window_spec))
                persons_raw_df = persons_raw_df.where(
                    (col("row_num") == 1) | ((col("row_num") > 1) & (col('id').isin(account_ids) == False)))
                persons_raw_df = persons_raw_df.drop("row_num")
                persons_raw_df.repartition(1).write.mode("overwrite").parquet(file_path + '_temp')
                s3_path = file_path + '_temp'
                path_parts = s3_path.replace("s3://", "").split("/")
                bucket = path_parts.pop(0)
                dir_key = "/".join(path_parts)

                find_obj = s3_client.list_objects(Bucket=bucket, Prefix=dir_key)
                file_key =[something for something in find_obj['Contents'] if (something['Key'].split('.')[-1] == 'parquet')][0]['Key']

                s3_client.Object(raw_bucket, file_path).copy_from(CopySource='s3://'+raw_bucket+file_key)
                s3_client.Object(raw_bucket, file_path + '_temp').delete()

        store_last_updated(lastupdated_table, 'GDPR_Delete_Raw_Salesforce_Users', time.mktime(current_date.timetuple()))
        i = i + 1


def delete_raw_salesforce_contacts(lastupdated_table, raw_bucket):
    # DELETES HISTORY OF ACCOUNTS TABLE RECORDS WHERE THE ACCOUNT ISANONYMIZED, LEAVING ONLY THE LATEST ANONYMIZED RECORD
    global spark, s3_client

    last_updated = get_last_updated(lastupdated_table, 'GDPR_Delete_Raw_Salesforce_Contacts')

    today = date.today()
    difference = (today - last_updated).days

    i = 1
    while i <= difference:
        current_date = last_updated + timedelta(days=i)
        year = str(current_date.year)
        month = str(current_date.month)
        day = str(current_date.day)

        query = 'SELECT userid FROM "customerintelligence"."config_gdpr_personsdeletionlog" A join "customerintelligence"."users" B on A.personid = b.personid  ' + \
                'where "year" = \'' + year + '\' and "month" = \'' + month + '\' and "day" = \'' + day + '\''
        account_ids = get_list_by_query(query)
        if len(account_ids) > 0:
            query = 'SELECT distinct(path) FROM (SELECT id, "$path" as path, ROW_NUMBER() OVER (PARTITION BY id ORDER BY cigcopytime DESC) AS row_num ' + \
                    'FROM "customerintelligence_raw"."object_contact") AS t WHERE row_num > 1 and userid__c IN ' + \
                    '(' + (','.join(list(map(lambda x: '\'' + str(x) + '\'', account_ids)))) + '))'
            file_paths = get_list_by_query(query)

            window_spec = Window.partitionBy(col('id')).orderBy(col('cigcopytime').desc())
            for file_path in file_paths:
                persons_raw_df = spark.read.parquet(file_path)
                persons_raw_df = persons_raw_df.withColumn('cigcopytime', col('cigcopytime').cast(TimestampType()))
                persons_raw_df = persons_raw_df.withColumn("row_num", row_number().over(window_spec))
                persons_raw_df = persons_raw_df.where(
                    (col("row_num") == 1) | ((col("row_num") > 1) & (col('id').isin(account_ids) == False)))
                persons_raw_df = persons_raw_df.drop("row_num")
                persons_raw_df.repartition(1).write.mode("overwrite").parquet(file_path + '_temp')
                s3_client.Object(raw_bucket, file_path).copy_from(CopySource=file_path + '_temp')
                s3_client.Object(raw_bucket, file_path + '_temp').delete()

        store_last_updated(lastupdated_table, 'GDPR_Delete_Raw_Salesforce_Contacts',
                           time.mktime(current_date.timetuple()))
        i = i + 1


if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, [
        'lastupdated_table',
        'athena_output_bucket',
        'raw_bucket',
        'athena_output_location',
        'raw_db'
    ])
    raw_db = args['raw_db']
    raw_bucket = args['raw_bucket']
    athena_output_location = args['athena_output_location']

    session = boto3.Session(region_name='eu-west-1')
    dynamodb_client = session.resource('dynamodb')
    lastupdated_table = dynamodb_client.Table(args['lastupdated_table'])

    athena_client = session.client('athena')
    s3_client = session.resource('s3')
    s3_client_ll = session.client('s3')
    delete_raw_accounts(lastupdated_table, raw_bucket)
    # anonymize_divisions(lastupdated_table, raw_bucket)
    # delete_salesforce_accounts(lastupdated_table, raw_bucket)
    # delete_raw_persons(lastupdated_table, raw_bucket)
    # delete_raw_users(lastupdated_table, raw_bucket)
    # delete_raw_salesforce_users(lastupdated_table, raw_bucket)
    # delete_raw_salesforce_contacts(lastupdated_table, raw_bucket)

    cleanup(args['athena_output_bucket'], athena_output_location)