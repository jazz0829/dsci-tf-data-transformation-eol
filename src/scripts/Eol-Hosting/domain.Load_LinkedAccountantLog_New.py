import sys
from utils import convertDataTypes, cleanDataFrame
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, last_day, lead, date_add, when, max, last
from pyspark.sql.window import Window

def load_linkedAccountantLog(raw_accountant_df, config_calendar_df):
    currentDate = date.today()

    accountant_df = raw_accountant_df.select('ID', 'Accountant', 'CIGCopyTime')
    accountant_df = convertDataTypes(
        data_frame=accountant_df,
        date_cols=['CIGCopyTime']
    )

    accountant_df = accountant_df.withColumn('CurrentDate', last_day(col('CIGCopyTime')))
    accountant_df = accountant_df.orderBy(col('CurrentDate').asc())

    window_spec = Window.partitionBy(col('ID')).orderBy(col('CurrentDate').asc())
    accountant_df = accountant_df.withColumn('NextDate', lead(col('CurrentDate'), 1, currentDate).over(window_spec))
    accountant_df = accountant_df.drop("CIGCopyTime")
    accountant_df = accountant_df.drop_duplicates()
    accountant_df = accountant_df.filter(col('CurrentDate') != col('NextDate'))

    calendar_df = config_calendar_df.select('CalendarDate', 'YearMonth')
    calendar_df = convertDataTypes(
        data_frame=calendar_df,
        integer_cols=['YearMonth'],
        date_cols=['CalendarDate']
    )

    trimmed_accountant_df = accountant_df.alias('acc').join(calendar_df.alias('cald'), 
                            col('cald.CalendarDate').between(col('acc.CurrentDate'), date_add(col('acc.NextDate'), -1)), 'inner')
    trimmed_accountant_df = trimmed_accountant_df.groupBy('ID', 'CalendarDate').agg(last('Accountant').alias('Accountant'))

    last_date_calendar_df = calendar_df
    last_date_calendar_df = last_date_calendar_df.groupBy('YearMonth').agg(max('CalendarDate').alias('CalendarDate'))

    result_df = trimmed_accountant_df.alias('sub').join(last_date_calendar_df.alias('cal'), col('sub.CalendarDate') == col('cal.CalendarDate'))
    result_df = result_df.select('sub.ID', 'sub.CalendarDate', 'sub.Accountant').orderBy('sub.ID', 'sub.CalendarDate')\
                            .withColumnRenamed('CalendarDate', 'Date')\
                            .withColumnRenamed('Accountant', 'AccountantID')\
                            .withColumnRenamed('ID', 'AccountID')

    return result_df

if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ['raw_db', 'domain_db', 'raw_eol_accounts_table',
                                     'config_calendar',
                                     's3_destination_linkedaccountantlog_new'])
    glueContext = GlueContext(SparkContext.getOrCreate())
    raw_accountant_df = glueContext.create_dynamic_frame.from_catalog(
        database=args['raw_db'],
        table_name=args['raw_eol_accounts_table']).toDF()
    raw_calendar_df = glueContext.create_dynamic_frame.from_catalog(
        database=args['domain_db'],
        table_name=args['config_calendar']).toDF()

    linkedAccountant_log_df = load_linkedAccountantLog(raw_accountant_df, raw_calendar_df)
    linkedAccountant_log_df.repartition(1).write.mode("overwrite").parquet(args['s3_destination_linkedaccountantlog_new'])