import sys
from utils import convertDataTypes, copy_parquets, store_last_updated, get_last_updated
from datetime import date, timedelta
import time
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, lit, coalesce
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StringType
import boto3

spark = SparkSession.builder.getOrCreate()
glueContext = GlueContext(SparkContext.getOrCreate())
keyevents_df = None
accounts_df = None
divisions_df = None
keyevents_lastupdated_table = None

today = date.today()

last_updates = {}

def store_last_updated_keyevent(keyevent_name, updated):
    global last_updates
    last_updates[keyevent_name] = int(updated)

def store_last_updated_items(table):
    global last_updates
    for event_name, updated in last_updates.items():
        store_last_updated(table, event_name, updated)

def load_keyevents_with_divisioncode(keyevent_name, where_condition, divisionstatistics_x_df, cte_a, cte_c):
    global keyevents_lastupdated_table, divisions_df, accounts_df

    last_updated = get_last_updated(keyevents_lastupdated_table, keyevent_name)

    joined_df = divisionstatistics_x_df.alias("dsd")\
                    .join(divisions_df.alias("d"), (col("dsd.divisioncode") == col("d.divisioncode")) & (col("dsd.environment") == col("d.environment")), "inner")\
                    .join(accounts_df.alias("a"), col("d.accountid") == col("a.accountid"), "inner")\
                    .where(where_condition)\
                    .groupBy("dsd.environment", "dsd.divisioncode")\
                    .agg({"dsd.date": "min"})\
                    .withColumnRenamed("min(date)", "min_date")\
                    .where((col("min_date") <= cte_c) & (col("min_date") > last_updated))

    new_keyevents_df = cte_a.alias("cte").join(divisions_df.alias("d"), col("d.accountid") == col("cte.accountid"), "inner")\
                                    .join(joined_df.alias("dsd"),  (col("dsd.divisioncode") == col("d.divisioncode")) & (col("dsd.environment") == col("d.environment")), "inner")\
                                    .groupBy("cte.accountid", "dsd.min_date", "cte.environment", "d.divisioncode")\
                                    .agg({"dsd.min_date": "min"})\
                                    .withColumn("inserteddatetime", lit(today))\
                                    .withColumn("userid", lit(None).cast(StringType()))\
                                    .withColumn("key_event", lit(keyevent_name))\
                                    .withColumnRenamed("min(min_date)", "key_event_timestamp")\
                                    .select("accountid", "environment", "divisioncode", "userid", "key_event", "key_event_timestamp", "inserteddatetime")

    save_keyevents(new_keyevents_df, keyevent_name)

def load_keyevents_firstconsult(cte_a, timecosttransactions_df, items_df):
    global keyevents_lastupdated_table, accounts_df

    keyevent_name = 'First Consult'

    last_updated = get_last_updated(keyevents_lastupdated_table, keyevent_name)

    joined_df = timecosttransactions_df.alias("tt")\
                    .join(items_df.alias("i"), col("tt.itemid") == col("i.itemid"), "inner")\
                    .join(accounts_df.alias("a"), col("tt.accountid") == col("a.accountid"), "inner")\
                    .where(col("i.itemcode").like('%EOLCONSULT%'))\
                    .groupBy("a.environment", "tt.accountid")\
                    .agg({"tt.date": "min"})\
                    .withColumnRenamed("min(date)", "min_date")\
                    .where((col("min_date") <= cte_c) & (col("min_date") > last_updated))
    
    new_keyevents_df = cte_a.alias("cte")\
                                    .join(joined_df.alias("con"),  (col("con.accountid") == col("cte.accountid")) & (col("con.environment") == col("cte.environment")), "inner")\
                                    .groupBy("cte.accountid", "con.min_date", "cte.environment")\
                                    .agg({"con.min_date": "min"})\
                                    .withColumn("inserteddatetime", lit(today))\
                                    .withColumn("userid", lit(None).cast(StringType()))\
                                    .withColumn("divisioncode", lit(None).cast(StringType()))\
                                    .withColumn("key_event", lit(keyevent_name))\
                                    .withColumnRenamed("min(min_date)", "key_event_timestamp")\
                                    .select("accountid", "environment", "divisioncode", "userid", "key_event", "key_event_timestamp", "inserteddatetime")

    save_keyevents(new_keyevents_df, keyevent_name)

def load_keyevents_firstconsultrequested(cte_a, requests_consultaanvraag_df):
    global keyevents_lastupdated_table, keyevents_df

    keyevent_name = 'First Consult Requested'

    last_updated = get_last_updated(keyevents_lastupdated_table, keyevent_name)

    joined_df = requests_consultaanvraag_df\
                    .where(col("status") != 'S')\
                    .groupBy("environment", "accountid")\
                    .agg({"created": "min"})\
                    .withColumnRenamed("min(created)", "min_date")\
                    .select('accountid', 'environment', 'min_date')
    
    new_keyevents_df = cte_a.alias("cte")\
                                    .join(joined_df.alias("con"),  (col("con.accountid") == col("cte.accountid")) & (col("con.environment") == col("cte.environment")), "inner")\
                                    .groupBy("cte.accountid", "con.min_date", "cte.environment")\
                                    .agg({"con.min_date": "min"})\
                                    .withColumn("inserteddatetime", lit(today))\
                                    .withColumn("userid", lit(None).cast(StringType()))\
                                    .withColumn("divisioncode", lit(None).cast(StringType()))\
                                    .withColumn("key_event", lit(keyevent_name))\
                                    .withColumnRenamed("min(min_date)", "key_event_timestamp")\
                                    .select("accountid", "environment", "divisioncode", "userid", "key_event", "key_event_timestamp", "inserteddatetime")

    keyevents_df = keyevents_df.union(new_keyevents_df)

    last_updated = keyevents_df.where(col("key_event") == keyevent_name).agg({"key_event_timestamp": "max"}).first()['max(key_event_timestamp)']

    store_last_updated_keyevent('First Consult Requested', time.mktime(last_updated.timetuple()))

def load_keyevents_firstmobileappused(cte_a, activitydaily_df):
    global keyevents_lastupdated_table

    keyevent_name = 'First Time Mobile App Used'

    last_updated = get_last_updated(keyevents_lastupdated_table, keyevent_name)

    new_keyevents_df = activitydaily_df.alias("ad")\
                                    .join(cte_a.alias("cte"),  (col("ad.accountid") == col("cte.accountid")), "inner")\
                                    .where(col("ad.activityid") == '8')\
                                    .groupBy("ad.accountid")\
                                    .agg({"ad.date": "min"})\
                                    .withColumn("inserteddatetime", lit(today))\
                                    .withColumn("userid", lit(None).cast(StringType()))\
                                    .withColumn("divisioncode", lit(None).cast(StringType()))\
                                    .withColumn("environment", lit(None).cast(StringType()))\
                                    .withColumn("key_event", lit(keyevent_name))\
                                    .withColumnRenamed("min(date)", "key_event_timestamp")\
                                    .select("accountid", "environment", "divisioncode", "userid", "key_event", "key_event_timestamp", "inserteddatetime")

    save_keyevents(new_keyevents_df, keyevent_name)

def load_keyevents_firsttimeaccountantlinked(cte_a, domain_db, domain_linkedaccountantlog_table_name):
    global keyevents_lastupdated_table

    linkedaccountantlog_df = glueContext.create_dynamic_frame.from_catalog(database = domain_db, table_name = domain_linkedaccountantlog_table_name)\
                                .select_fields(['accountcode', 'environment', 'linkstatus', 'date'])\
                                .toDF()
    keyevent_name = 'First Time Accountant Linked'

    last_updated = get_last_updated(keyevents_lastupdated_table, keyevent_name)

    cte_c = linkedaccountantlog_df.agg({"date": "max"}).first()['max(date)']

    new_keyevents_df = cte_a.alias("cte")\
                                    .join(linkedaccountantlog_df.alias("lal"),  (col("lal.environment") == col("cte.environment")) & (col("lal.accountcode") == col("cte.accountcode")), "inner")\
                                    .where(col("lal.linkstatus") == "Accountant Linked")\
                                    .groupBy("cte.accountid")\
                                    .agg({"lal.date": "min"})\
                                    .withColumn("inserteddatetime", lit(today))\
                                    .withColumn("userid", lit(None).cast(StringType()))\
                                    .withColumn("divisioncode", lit(None).cast(StringType()))\
                                    .withColumn("environment", lit(None).cast(StringType()))\
                                    .withColumn("key_event", lit(keyevent_name))\
                                    .withColumnRenamed("min(date)", "key_event_timestamp")\
                                    .select("accountid", "environment", "divisioncode", "userid", "key_event", "key_event_timestamp", "inserteddatetime")

    save_keyevents(new_keyevents_df, keyevent_name)

def load_keyevents_legislationtemplate(cte_c, divisionstatistics_summary_df):
    global keyevents_lastupdated_table, divisions_df
    
    keyevent_name = 'Legislation Template Created'
    last_updated = get_last_updated(keyevents_lastupdated_table, keyevent_name)

    window_spec = Window.partitionBy(col('d.accountid')).orderBy(col('d.syscreated').desc())

    new_keyevents_df = divisions_df.alias("d")\
                    .join(divisionstatistics_summary_df.alias("dss"), (col("d.divisioncode") == col("dss.divisioncode")) & (col("d.environment") == col("dss.environment")), "inner")\
                    .withColumn("row_num", row_number().over(window_spec))\
                    .where(
                        (col("row_num") == 1) & 
                        (col("dss.startuptype") == 'X') & #LegislationTemplate
                        (col("d.syscreated") <= cte_c) & 
                        (col("d.syscreated") > last_updated)
                    )\
                    .withColumn("userid", lit(None).cast(StringType()))\
                    .withColumn("key_event", lit(keyevent_name))\
                    .withColumn("inserteddatetime", lit(today))\
                    .select(col("d.accountid"), col("d.environment"), col("d.divisioncode"), col("userid"), col("key_event"), col("d.syscreated").alias("key_event_timestamp"), col("inserteddatetime"))

    save_keyevents(new_keyevents_df, keyevent_name)

def save_keyevents(new_keyevents_df, keyevent_name):
    global keyevents_df

    keyevents_df = keyevents_df.union(new_keyevents_df)

    window_spec = Window.partitionBy(col('accountid'), col("environment"), col("key_event"), col("divisioncode")).orderBy(col('key_event_timestamp').desc())

    keyevents_df = keyevents_df\
                                .withColumn("row_num", row_number().over(window_spec)).where((col("key_event") != keyevent_name) | ((col("key_event") == keyevent_name) & (col("row_num") == 1)))\
                                .drop("row_num")

    last_updated = keyevents_df.where(col("key_event") == keyevent_name).agg({"key_event_timestamp": "max"}).first()['max(key_event_timestamp)']

    store_last_updated_keyevent(keyevent_name, time.mktime(last_updated.timetuple()))

if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, [
        'raw_db', 
        'domain_db',
        's3_bucket',
        'domain_keyevents_all_table',
        'domain_divisions_table',
        'domain_divisionstatistics_daily_table',
        'domain_accountscontract_summary_table',
        'domain_accounts_table',
        'domain_requests_consultaanvraag_table',
        'domain_timecosttransactions_table',
        'domain_items_table',
        'domain_activitydaily_table',
        'domain_divisionstatistics_summary_table',
        'domain_linkedaccountantlog_table',
        'raw_eol_divisionstatistics_table',
        'lastupdated_table',
        's3_destination'
    ])
    raw_db = args['raw_db']
    domain_db = args['domain_db']
    keyevents_df = glueContext.create_dynamic_frame.from_catalog(database = domain_db, table_name = args['domain_keyevents_all_table']).toDF()
    keyevents_df.cache()
    accountscontract_summary_df = glueContext.create_dynamic_frame.from_catalog(database = domain_db, table_name = args['domain_accountscontract_summary_table'])\
                                    .select_fields(['accountid', 'environment', 'accountcode', 'firsttrialstartdate', 'firstcommstartdate'])\
                                    .toDF()
    accounts_df = glueContext.create_dynamic_frame.from_catalog(database = domain_db, table_name = args['domain_accounts_table'])\
                                    .select_fields(['accountid', 'environment'])\
                                    .toDF()
    accounts_df.cache()
    divisionstatistics_raw_df = glueContext.create_dynamic_frame.from_catalog(database = raw_db, table_name = args['raw_eol_divisionstatistics_table'])\
                                    .select_fields(['cigcopytime'])\
                                    .toDF()
    divisionstatistics_daily_df = glueContext.create_dynamic_frame.from_catalog(database = domain_db, table_name = args['domain_divisionstatistics_daily_table'])\
                                    .select_fields([
                                        'divisioncode', 'environment', 'date', 'accountcount', 'ingbankaccounts', 'rabobankaccounts', 'abnamrobankaccounts',
                                        'bankentrycountmanual', 'automaticbanklink', 'banktransactionsimport', 'opportunitycount', 'projecttypefixedprice',
                                        'projecttypetimeandmaterial', 'projecttypenonbillable', 'projecttypeprepaidhtb', 'projecttypeprepaidretainer', 
                                        'projectsalesinvoices', 'purchaseentrycount', 'projecttotaltimeentries'
                                        ])\
                                    .toDF()
    divisionstatistics_daily_df.cache()
    divisions_df = glueContext.create_dynamic_frame.from_catalog(database = domain_db, table_name = args['domain_divisions_table'])\
                                    .select_fields(['divisioncode', 'environment', 'accountid', 'syscreated'])\
                                    .toDF()
    divisions_df.cache()
    
    session = boto3.Session(region_name='eu-west-1')
    dynamodb = session.resource('dynamodb')
    keyevents_lastupdated_table = dynamodb.Table(args['lastupdated_table'])

    cte_a = accountscontract_summary_df.alias("acs").join(accounts_df.alias("a"), col("acs.accountid") == col("a.accountid"), "inner")\
                                    .select("acs.environment", "acs.accountcode", "acs.accountid", "acs.firsttrialstartdate", "acs.firstcommstartdate")

    cte_c = convertDataTypes(
            data_frame = divisionstatistics_raw_df,
            timestamp_cols = ['cigcopytime']
        ).agg({"cigcopytime": "min"}).first()['min(cigcopytime)']
    
    where_condition = col("dsd.accountcount") > 0
    load_keyevents_with_divisioncode('First Account Card', where_condition, divisionstatistics_daily_df, cte_a, cte_c)

    where_condition = (col("dsd.ingbankaccounts") > 0) | (col("dsd.rabobankaccounts") > 0) | (col("dsd.abnamrobankaccounts") > 0)
    load_keyevents_with_divisioncode('First Bank Account', where_condition, divisionstatistics_daily_df, cte_a, cte_c)

    where_condition = (col("dsd.bankentrycountmanual") > 0)
    load_keyevents_with_divisioncode('First Bank Entry Manual', where_condition, divisionstatistics_daily_df, cte_a, cte_c)

    where_condition = (col("dsd.automaticbanklink") > 0)
    load_keyevents_with_divisioncode('First Bank Link', where_condition, divisionstatistics_daily_df, cte_a, cte_c)

    where_condition = (col("dsd.banktransactionsimport") > 0)
    load_keyevents_with_divisioncode('First Bank Statement Import', where_condition, divisionstatistics_daily_df, cte_a, cte_c)

    timecosttransactions_df = glueContext.create_dynamic_frame.from_catalog(database = domain_db, table_name = args['domain_timecosttransactions_table'])\
                                                .select_fields(['itemid', 'accountid', 'date'])\
                                                .toDF()

    items_df = glueContext.create_dynamic_frame.from_catalog(database = domain_db, table_name = args['domain_items_table'])\
                            .select_fields(['itemid', 'itemcode'])\
                            .toDF()

    load_keyevents_firstconsult(cte_a, timecosttransactions_df, items_df)

    requests_consultaanvraag_df = glueContext.create_dynamic_frame.from_catalog(database = domain_db, table_name = args['domain_requests_consultaanvraag_table'])\
                                                .select_fields(['status', 'environment', 'accountid', 'created'])\
                                                .toDF()

    load_keyevents_firstconsultrequested(cte_a, requests_consultaanvraag_df)

    activitydaily_df = glueContext.create_dynamic_frame.from_catalog(database = domain_db, table_name = args['domain_activitydaily_table'])\
                                        .select_fields(['accountid', 'activityid', 'date'])\
                                        .toDF()

    load_keyevents_firstmobileappused(cte_a, activitydaily_df)

    where_condition = (col("dsd.opportunitycount") > 0)
    load_keyevents_with_divisioncode('First Opportunity', where_condition, divisionstatistics_daily_df, cte_a, cte_c)

    where_condition = (
        (coalesce(col("dsd.projecttypefixedprice"), lit(0)) + 
        coalesce(col("dsd.projecttypetimeandmaterial"), lit(0)) + 
        coalesce(col("dsd.projecttypenonbillable"), lit(0)) + 
        coalesce(col("dsd.projecttypeprepaidhtb"), lit(0)) + 
        coalesce(col("dsd.projecttypeprepaidretainer"), lit(0))) > 0
    )
    load_keyevents_with_divisioncode('First Project', where_condition, divisionstatistics_daily_df, cte_a, cte_c)

    where_condition = (col("dsd.projectsalesinvoices") > 0)
    load_keyevents_with_divisioncode('First Project Sales Invoice', where_condition, divisionstatistics_daily_df, cte_a, cte_c)

    where_condition = (col("dsd.purchaseentrycount") > 0)
    load_keyevents_with_divisioncode('First Purchase Entry', where_condition, divisionstatistics_daily_df, cte_a, cte_c)

    divisionstatistics_summary_df = glueContext.create_dynamic_frame.from_catalog(database = domain_db, table_name = args['domain_divisionstatistics_summary_table'])\
                                                    .select_fields(['divisioncode', 'environment', 'startuptype', 'salesinvoicefirstdate'])\
                                                    .toDF()
    divisionstatistics_summary_df = divisionstatistics_summary_df.withColumn('date', col('salesinvoicefirstdate'))
    where_condition = (col("dsd.salesinvoicefirstdate").isNotNull())
    load_keyevents_with_divisioncode('First Sales Invoice', where_condition, divisionstatistics_summary_df, cte_a, cte_c)

    load_keyevents_firsttimeaccountantlinked(cte_a, domain_db, args['domain_linkedaccountantlog_table'])

    where_condition = (col("dsd.projecttotaltimeentries") > 0)
    load_keyevents_with_divisioncode('First Time Entry', where_condition, divisionstatistics_daily_df, cte_a, cte_c)

    load_keyevents_legislationtemplate(cte_c, divisionstatistics_summary_df)

    keyevents_df.repartition(1).write.mode("overwrite").parquet(args['s3_destination']+"_Temp")
    copy_parquets(args['s3_bucket'], "Data/KeyEvents_All_Temp", "Data/KeyEvents_All")

    store_last_updated_items(keyevents_lastupdated_table)