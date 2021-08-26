import sys
import json
import boto3
from utils import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, trim, upper, concat, substring, lpad, lit

def load_requests (settings):
    print('Running {0} transformation ...'.format(settings['name']))
    window_spec = Window.partitionBy(col('id')).orderBy(col('cigcopytime').desc())
    result_df = requests_raw_df.where(settings['filter_condition'])
    result_df = result_df.withColumn("row_num", row_number().over(window_spec))
    result_df = result_df.where((col('row_num') == 1))

    for column in settings['columns_to_drop']:
        result_df = result_df.drop(column)

    result_df = renameColumns(result_df, settings['columns_to_rename'])

    result_df = result_df.withColumn('requesthid', lpad("requesthid", 8, '0'))
    result_df = result_df.withColumn('requestcode', concat(
        substring(col('requesthid'), 1, 2), lit('.'), substring(col('requesthid'), 3, 3), lit('.'),
        substring(col('requesthid'), 6, 8)))

    result_df = convertDataTypes(
        data_frame = result_df,
        timestamp_cols = settings['timestamp_columns'],
        integer_cols = settings['integer_columns'],
        double_cols = settings['double_cols'],
        date_cols = settings['date_columns'],
        boolean_cols = settings['boolean_cols']
    )
    result_df = result_df.select(settings['columns_to_select'])
    if result_df.count() > 0:
        result_df.repartition(1).write.mode("overwrite").parquet(settings['s3_destination'])

if __name__ == "__main__":
    glueContext = GlueContext(SparkContext.getOrCreate())
    s3 = boto3.resource('s3')
    args = getResolvedOptions(sys.argv, [
        'raw_db',
        'raw_eol_requests_table',
        's3_artifact_bucket',
        'configuration_file_key'
    ])

    requests_raw_df = glueContext.create_dynamic_frame.from_catalog(database=args['raw_db'],
                                                                    table_name=args['raw_eol_requests_table']).toDF()

    upper_cols = ['id', 'account', 'contact','syscreator' , 'sysmodifier', 'approver', 'type', 'userid','realizer',
                  'environment','document']
    requests_raw_df = cleanDataFrame(requests_raw_df, to_upper_cols = upper_cols)
    requests_raw_df.cache()

    content_object = s3.Object(args['s3_artifact_bucket'], args['configuration_file_key'])
    file_content = content_object.get()['Body'].read().decode('utf-8')
    data = json.loads(file_content )

    for config in data['settings']:
        settings = {
            "name": config['name'],
            "columns_to_rename": config['columns_to_rename'],
            "columns_to_select": config['columns_to_select'],
            "columns_to_drop": config['columns_to_drop'],
            "filter_condition": config['filter_condition'],
            "timestamp_columns": config['timestamp_columns'],
            "integer_columns": config['integer_columns'],
            "date_columns": config['date_columns'],
            "double_cols": config['double_cols'],
            "boolean_cols": config['boolean_cols'],
            "s3_destination": config['s3_destination']
        }
        load_requests(settings)
