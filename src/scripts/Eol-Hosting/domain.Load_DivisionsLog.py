import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from pyspark.sql.functions import when, lit, col, concat, trim, \
    lag, lead, upper, row_number, current_date, to_date, max, year, month, lpad, min
from awsglue.utils import getResolvedOptions
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, StringType, BooleanType, DateType
from utils import convertDataTypes

def load_divisionlog(raw_divisions_DF):
    default_date = datetime(1900, 1, 1)
    default_date_1990 = datetime(1990, 1, 1)
    default_date_2016_1 = datetime(2016, 11, 15)
    default_date_2016_2 = datetime(2016, 11, 14)

    window_spec = Window.partitionBy(col('code'), col('environment')).orderBy(col('cigcopytime').asc())
    divisions_DF = convertDataTypes(
        data_frame=raw_divisions_DF,
        integer_cols=['blockingstatus', 'status'],
        timestamp_cols=[
            'cigcopytime', 'deleted', 'enddate']
    )
    divisions_DF = divisions_DF.withColumn('code', trim(col('code')))
    divisions_DF = divisions_DF.withColumn('environment', upper(trim(col('environment'))))
    divisions_DF = divisions_DF.withColumn("row_num", row_number().over(window_spec))
    divisions_DF = divisions_DF.withColumn("lag", lag(divisions_DF.blockingstatus, 1, 999).over(window_spec))
    divisions_DF = divisions_DF.withColumn("lead", lead(divisions_DF.blockingstatus, 1, 999).over(window_spec))
    divisions_DF = divisions_DF.withColumn("lagstatus", lag(divisions_DF.status, 1, 999).over(window_spec))
    divisions_DF = divisions_DF.withColumn("leadstatus", lead(divisions_DF.status, 1, 999).over(window_spec))
    divisions_DF = divisions_DF.withColumn("lagdeleted", lag(divisions_DF.deleted, 1, default_date).over(window_spec))
    divisions_DF = divisions_DF.withColumn("leaddeleted", lead(divisions_DF.deleted, 1, default_date).over(window_spec))
    divisions_DF = divisions_DF.withColumn("lagenddate", lag(divisions_DF.enddate, 1, default_date).over(window_spec))
    divisions_DF = divisions_DF.withColumn("leadenddate", lead(divisions_DF.enddate, 1, default_date).over(window_spec))

    divisions_DF = divisions_DF.filter(
        ((col('blockingstatus') == 101) & (col('lag') <= 100))
        | ((col('blockingstatus') == 0) & (col('lag') == 999))
        | ((col('status') == 1) & (col('lagstatus') == 0) & ((col('leadstatus') != 0) | (col('leadstatus') == 999)))
        | ((col('status') == 0) & (col('lagstatus') == 999))
        | ((col('status') == 1) & (col('lagstatus') == 999))
        | ((col('deleted').isNotNull()) & (col('lagdeleted').isNull()) &
           ((col('leaddeleted').isNotNull()) | (col('leaddeleted') != default_date)))
        | ((col('deleted').isNull()) & (col('lagdeleted') == default_date))
        | ((col('enddate').isNotNull()) & (col('lagenddate').isNull()) &
           ((col('leadenddate').isNotNull()) | (col('leadenddate') != default_date)))
        | ((col('enddate').isNull()) & (col('lagenddate') == default_date))
        | ((col('row_num') == 1) | (col('row_num') == 2))
    )

    divisions_DF = divisions_DF.withColumn('event',
            when(
                (col('row_num') == 1) &
                (to_date(col('cigcopytime')) != default_date_2016_1.date()) &
                (col('blockingstatus') == 101), "startdate")
            .when(
                (col('row_num') == 1) &
                ((to_date(col('cigcopytime')) == default_date_2016_1.date()) |
                    ((to_date(col('cigcopytime')) == default_date_2016_2.date()) & (col('environment') == "US"))) &
                (col('blockingstatus') == 101), "startdate")
            .when(
                (col('row_num') == 2) &
                (to_date(col('cigcopytime')) > default_date_2016_1.date()) &
                (col('blockingstatus') == 101) & (col('Deleted').isNotNull()), "deleted")
            .when(
                (col('row_num') == 2) &
                ((to_date(col('cigcopytime')) > default_date_2016_1.date()) | (
                    (to_date(col('cigcopytime')) == default_date_2016_2.date()) & (col('environment') == "US"))) &
                (col('blockingstatus') == 101) & (col('Deleted').isNotNull()), "deleted")
            .when(
                (col('row_num') == 2) &
                (to_date(col('cigcopytime')) > default_date_2016_1.date()) &
                (col('enddate').isNotNull()) &
                (col('enddate') < current_date()), "ended")
            .when(
                (col('row_num') == 2) &
                ((to_date(col('cigcopytime')) > default_date_2016_1.date()) | (
                    (to_date(col('cigcopytime')) == default_date_2016_2.date()) & (col('environment') == "US"))) &
                (col('enddate').isNotNull()) &
                (col('enddate') < current_date()), "ended")
            .when(
                (col('row_num') == 2) &
                (to_date(col('cigcopytime')) > default_date_2016_1.date()) &
                (col('blockingstatus') == 101), "blocked")
            .when(
                (col('row_num') == 2) &
                ((to_date(col('cigcopytime')) > default_date_2016_1.date()) | (
                    (to_date(col('cigcopytime')) == default_date_2016_2.date()) & (col('environment') == "US"))) &
                (col('blockingstatus') == 101), "blocked")
            .when(
                (col('row_num') == 3) &
                (to_date(col('cigcopytime')) > default_date_2016_1.date()) &
                (col('blockingstatus') == 101), "blocked")
            .when(
                (col('row_num') == 1) &
                (to_date(col('cigcopytime')) != default_date_2016_1.date()), "startdate")
            .when(
                (col('row_num') == 1) &
                ((to_date(col('cigcopytime')) == default_date_2016_1.date()) | (
                (to_date(col('cigcopytime')) == default_date_2016_2.date()) & (col('environment') == "US"))), "startdate")
            .when(
                (col('blockingstatus') == 101) &
                (col('lag') == 0) &
                (col('deleted') > default_date_1990) &
                (col('lagdeleted').isNull()), "blockedanddeleted")
            .when(
                (col('blockingstatus') == 101) &
                (col('lag') == 0) &
                (col('deleted') > default_date_1990) &
                (col('lagenddate').isNull()), "blockedanddeleted")
            .when(
                (col('blockingstatus') == 101) &
                (col('lag') == 100) &
                (col('deleted') > default_date_1990), "blockedanddeleted")
            .when((col('blockingstatus') == 101) & (col('lag') == 0), "blocked")
            .when((col('status') == 1) & (col('lagstatus') == 0), "archived")
            .when((col('status') == 1) & (col('lagstatus') == 999), "archived")
            .when((col('status') == 1) & (col('lagstatus') == 1), "archived")
            .when((col('deleted') > default_date_1990) & (col('lagdeleted').isNull()), "deleted")
            .when((col('enddate') > default_date_1990) & (col('lagenddate').isNull()), "ended")
            .otherwise(lit(None)))

    divisions_DF = divisions_DF.withColumn('eventdate',
        when(
            (col('row_num') == 1) &
            (to_date(col('cigcopytime')) != default_date_2016_1.date()) &
            (col('syscreated') < default_date_2016_1), col('syscreated'))
        .when(
            (col('row_num') == 1) &
            ((to_date(col('cigcopytime')) != default_date_2016_1.date()) & (col('environment') == "US")), col('syscreated'))
        .when(
            (col('row_num') == 2) &
            (to_date(col('cigcopytime')) > default_date_2016_1.date()) &
            (col('blockingstatus') == 101) &
            (col('deleted').isNotNull()), col('deleted'))
        .when(
            (col('row_num') == 2) &
            ((to_date(col('cigcopytime')) > default_date_2016_1.date()) & (col('environment') == "US")) &
            (col('blockingstatus') == 101) &
            (col('deleted').isNotNull()), col('deleted'))
        .when(
            (col('row_num') == 2) &
            (to_date(col('cigcopytime')) > default_date_2016_1.date()) &
            (col('enddate').isNotNull()) &
            (col('enddate') < current_date()), col('enddate'))
        .when(
            (col('row_num') == 2) &
            ((to_date(col('cigcopytime')) > default_date_2016_1.date()) &
             (col('environment') == "US")) &
            (col('enddate').isNotNull()) &
            (col('enddate') < current_date()), col('deleted'))
        .when(
            (col('row_num') == 2) &
            (to_date(col('cigcopytime')) > default_date_2016_1.date()) &
            (col('blockingstatus') == 101), col('sysmodified'))
        .when(
            (col('row_num') == 2) &
            ((to_date(col('cigcopytime')) > default_date_2016_1.date()) |
                ((to_date(col('cigcopytime')) == default_date_2016_2.date()) & (col('environment') == "US"))) &
            (col('blockingstatus') == 101), col('sysmodified'))
        .when(
            (col('row_num') == 3) &
            (to_date(col('cigcopytime')) > default_date_2016_1.date()) &
            (col('blockingstatus') == 101), col('sysmodified'))
        .when(
            (col('row_num') == 1) &
            (to_date(col('cigcopytime')) != default_date_2016_1.date()), col('syscreated'))
        .when(
            (col('row_num') == 1) &
            ((to_date(col('cigcopytime')) == default_date_2016_1.date()) | (
                (to_date(col('cigcopytime')) == default_date_2016_2.date()) & (col('environment') == "US"))), col('syscreated'))
        .when(
            (col('blockingstatus') == 101) &
            (col('lag') == 0) &
            (col('deleted') > default_date_1990) &
            (col('lagdeleted').isNull()), col('deleted'))
        .when(
            (col('blockingstatus') == 101) &
            (col('lag') == 0) &
            (col('enddate') > default_date_1990) &
            (col('lagenddate').isNull()), col('enddate'))
        .when(
            (col('blockingstatus') == 101) & (col('lag') == 100) & (col('deleted') > default_date_1990), col('deleted'))
        .when((col('blockingstatus') == 101) & (col('lag') == 0), col('cigcopytime'))
        .when((col('status') == 1) & (col('lagstatus') == 0), col('cigcopytime'))
        .when((col('status') == 1) & (col('lagstatus') == 999), col('sysmodified'))
        .when((col('status') == 1) & (col('lagstatus') == 1), col('sysmodified'))
        .when((col('deleted') > default_date_1990) & (col('lagdeleted').isNull()),
             col('deleted'))
        .when((col('enddate') > default_date_1990) & (col('lagenddate').isNull()),
             col('enddate'))
        .otherwise(lit(None)))

    divisions_DF = divisions_DF.filter(col('event').isNotNull())
    trimmed_division_df = divisions_DF.select("code", "environment", "event", "eventdate")
    div_pivot_df = trimmed_division_df.groupBy("code", "environment").pivot("event").agg(min("eventdate"))
    div_pivot_df = div_pivot_df.withColumnRenamed('code', 'divisioncode')
    div_pivot_df = convertDataTypes(
        data_frame=div_pivot_df,
        integer_cols=['divisioncode'],
        timestamp_cols=[
            'deleted', 'blocked', 'archived', 'startdate', 'blockedanddeleted', 'ended']
    )

    div_pivot_df = div_pivot_df.withColumn("yearmonthdeleted",
                                           concat(year(col('deleted')), lpad(month(col('deleted')), 2, '0'))
                                           .cast(IntegerType()))
    div_pivot_df = div_pivot_df.withColumn("yearmonthblocked",
                                           concat(year(col('blocked')), lpad(month(col('blocked')), 2, '0'))
                                           .cast(IntegerType()))
    div_pivot_df = div_pivot_df.withColumn("yearmontharchived",
                                           concat(year(col('archived')), lpad(month(col('archived')), 2, '0'))
                                           .cast(IntegerType()))
    div_pivot_df = div_pivot_df.withColumn("yearmonthblockedanddeleted",
                                            concat(year(col('blockedanddeleted')), lpad(month(col('blockedanddeleted')), 2, '0'))
                                           .cast(IntegerType()))
    div_pivot_df = div_pivot_df.withColumn("yearmonthstarted",
                                           concat(year(col('startdate')),lpad(month(col('startdate')), 2,'0'))
                                           .cast(IntegerType()))
    div_pivot_df = div_pivot_df.withColumn("yearmonthended",
                                           concat(year(col('ended')), lpad( month(col('ended')),2, '0'))
                                           .cast(IntegerType()))
    return div_pivot_df


if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'raw_db', 'raw_eol_divisions_table', 's3_destination_division_log'])
    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session
    raw_divisions_DF = glueContext.create_dynamic_frame.from_catalog(
        database=args['raw_db'],
        table_name=args['raw_eol_divisions_table']).toDF()
    division_log_df = load_divisionlog(raw_divisions_DF)
    division_log_df.repartition(1).write.mode("overwrite").parquet(args['s3_destination_division_log'])
