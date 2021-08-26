import sys
from utils import *
from datetime import date, timedelta, datetime
import time

import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext

from pyspark.context import SparkContext
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, upper
from pyspark.sql.session import SparkSession

spark = SparkSession.builder.getOrCreate()
glueContext = GlueContext(SparkContext.getOrCreate())

def load_contracts(database, table_name, s3_destination, contracts_new_view_s3_destination):
    contracts_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()
    contracts_raw_df = cleanDataFrame(contracts_raw_df, ['id','environment'])
    contracts_raw_df = convertDataTypes(
        data_frame = contracts_raw_df,
        timestamp_cols=['startdate','enddate','finaldate','cancellationdate','cigcopytime','syscreated', 'sysmodified'],
        boolean_cols=['cigprocessed']
    )

    window_spec = Window.partitionBy(col('id')).orderBy(col('cigcopytime').desc())
    contracts_df = contracts_raw_df.withColumn("row_num", row_number().over(window_spec))
    the_latest_record_index = 1
    contracts_df = contracts_df.where(col('row_num') == the_latest_record_index)
    contracts_df = contracts_df.drop('cigcopytime','cigprocessed','row_num', 'ingestionday', 'ingestionmonth', 'ingestionyear')
    contracts_df.repartition(2).write.mode("overwrite").parquet(s3_destination)

    selectedcolumns = ['id', 'number', 'account', 'type', 'startdate', 'contractevent', 'enddate', 'finaldate',
                       'cancellationdate', 'cancellationevent', 'cigcopytime', 'environment', 'cigprocessed', 'timeunit',
                        'syscreated', 'syscreator', 'sysmodified', 'sysmodifier']

    contracts_new_view = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()
    contracts_new_view = contracts_new_view.select(selectedcolumns)

    contracts_new_view.repartition(2).write.mode("overwrite").parquet(contracts_new_view_s3_destination)

def load_accounts(database, table_name, s3_destination,accounts_hosting_view_s3_destination):
    accounts_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()
    accounts_raw_df = cleanDataFrame(accounts_raw_df, ['id'])

    accounts_raw_df = convertDataTypes(
            data_frame = accounts_raw_df,
            boolean_cols = ['isbank', 'ispilot', 'issupplier', 'issales', 'ispurchase', 'isreseller', 'ismember', 'blocked', 'purchaseorderconfirmation', 'reminder', 'backorders', 'attention', 'candropship', 'cigprocessed', 'processedbybackgroundjob', 'recepientofcommissions'],
            integer_cols = ['division', 'isaccountant', 'iscompetitor', 'invoicecopies', 'isagency', 'usetimespecification', 'ismailing', 'costpaid', 'separateinvperproject', 'separateinvpersubscription', 'consolidationscenario', 'securitylevel', 'quotationmarkuptype', 'shippingleaddays', 'purchaseleaddays', 'maindivision', 'customerdivision', 'rating', 'isanonymized', 'anonymisationsource'],
            timestamp_cols = ['cigcopytime', 'syscreated', 'sysmodified', 'controlleddate', 'checkdate', 'startdate', 'enddate', 'customersince', 'statussince', 'webaccesssince'],
            date_cols = ['datemodifiedbycoc', 'establisheddate'],
            double_cols = ['creditlinesales', 'creditlinepurchase', 'discountsales', 'discountpurchase', 'profitpercentage', 'labormarkuppercentage', 'purchasemarkuppercentage', 'purchasemarkuppercentageproject', 'longitude', 'latitude']
        )

    window_spec = Window.partitionBy(col('division'), col('id')).orderBy(col('cigcopytime').desc())
    accounts_df = accounts_raw_df.withColumn("row_num", row_number().over(window_spec))
    accounts_df = accounts_df.where(col('row_num') == 1)

    accounts_df = accounts_df.drop('row_num', 'ingestionday', 'ingestionmonth', 'ingestionyear', 'county', 'geolocation')
    accounts_df.repartition(1).write.mode("overwrite").parquet(s3_destination)

    accounts_hosting_view = accounts_df.drop('Email', 'Phone', 'PhoneExtension', 'PhoneSales', 'PhoneHelpdesk')
    accounts_hosting_view.repartition(1).write.mode("overwrite").parquet(accounts_hosting_view_s3_destination)



def load_paymentterms(database, table_name, s3_destination):
    paymentterms_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()
    paymentterms_raw_df = cleanDataFrame(paymentterms_raw_df, ['id'])

    paymentterms_raw_df = convertDataTypes(
            data_frame = paymentterms_raw_df,
            boolean_cols = ['transactionreversal'],
            integer_cols = ['division', 'linetype', 'endyear', 'endperiod', 'entrynumber', 'invoicenumber', 'status', 'source', 'paymentdays', 'transactiontype', 'transactionreportingyear', 'transactionreportingperiod', 'transactionstatus'],
            timestamp_cols = ['entrydate', 'invoicedate', 'duedate', 'enddate', 'transactionduedate', 'syscreated', 'sysmodified', 'cigcopytime'],
            double_cols = ['amountfc', 'ratefc', 'amountdc', 'paymentdiscount', 'transactionamountfc', 'transactionamountdc']
        )

    window_spec = Window.partitionBy(col('id')).orderBy(col('cigcopytime').desc())
    paymentterms_df = paymentterms_raw_df.withColumn("row_num", row_number().over(window_spec))
    paymentterms_df = paymentterms_df.where((col('row_num') == 1) & (col('cigcopytime') >= datetime(year=2018, month=10, day=5)))
    # This is the date that Infra provided us with a full backup of the PaymentTerms data that no longer contained the rows which caused the 10 cent A/R bug

    paymentterms_df = renameColumns(paymentterms_df, {
        'id':'paymenttermsid',
        'account': 'accountid',
        'division': 'divisioncode'
    })

    paymentterms_df = paymentterms_df.drop('row_num', 'ingestionday', 'ingestionmonth', 'ingestionyear', 'cigprocessed')
    paymentterms_df.repartition(5).write.mode("overwrite").parquet(s3_destination)

def load_persons_email(database, table_name, s3_destination):
    persons_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()
    persons_raw_df = cleanDataFrame(persons_raw_df, ['id'])

    persons_raw_df = convertDataTypes(
            data_frame = persons_raw_df,
            timestamp_cols = ['cigcopytime']
        )

    window_spec = Window.partitionBy(col('id')).orderBy(col('cigcopytime').desc())
    persons_email_df = persons_raw_df.withColumn("row_num", row_number().over(window_spec))
    persons_email_df = persons_email_df.where((col('row_num') == 1))

    persons_email_df = renameColumns(persons_email_df, {
        'id':'personid'
    })

    persons_email_df = persons_email_df.select('personid', 'email')
    persons_email_df.repartition(1).write.mode("overwrite").parquet(s3_destination)

def load_users_googleclientid(database, table_name, s3_destination):
    users_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()
    users_raw_df = cleanDataFrame(users_raw_df, ['id'])

    users_raw_df = convertDataTypes(
            data_frame = users_raw_df,
            timestamp_cols = ['cigcopytime']
        )

    window_spec = Window.partitionBy(col('id')).orderBy(col('cigcopytime').desc())
    users_googleclientid_df = users_raw_df.withColumn("row_num", row_number().over(window_spec))
    users_googleclientid_df = users_googleclientid_df.where((col('row_num') == 1))

    users_googleclientid_df = renameColumns(users_googleclientid_df, {
        'id':'userid'
    })

    users_googleclientid_df = users_googleclientid_df.select('userid', 'googleclientid')
    users_googleclientid_df.repartition(1).write.mode("overwrite").parquet(s3_destination)

def load_anonymized_accounts(database, table_name, s3_destination, lastupdated_table):
    last_updated = get_last_updated(lastupdated_table, 'Anonymized_Accounts')

    today = date.today()

    difference = (today - last_updated).days

    i = 1
    while i <= difference:
        current_date = last_updated + timedelta(days=i)
        year = str(current_date.year)
        month = str(current_date.month)
        day = str(current_date.day)

        push_down_filter = 'IngestionYear = ' + year + ' and IngestionMonth = ' + month + ' and IngestionDay = ' + day

        accounts_raw_df = glueContext.create_dynamic_frame.from_catalog(
            database=database, 
            table_name=table_name, 
            push_down_predicate=push_down_filter
        ).toDF()
        anonymized_df = accounts_raw_df.where(col("isanonymized") == '1').select(col("id").alias("accountid"))
        anonymized_df.repartition(1).write.mode("overwrite").parquet(s3_destination + '/year=' + year + '/month=' + month + '/day=' + day)
        store_last_updated(lastupdated_table, 'Anonymized_Accounts', time.mktime(current_date.timetuple()))
        i = i + 1

def load_anonymized_persons(database, table_name, s3_destination, lastupdated_table):
    last_updated = get_last_updated(lastupdated_table, 'Anonymized_Persons')

    today = date.today()

    difference = (today - last_updated).days

    i = 1
    while i <= difference:
        current_date = last_updated + timedelta(days=i)
        year = str(current_date.year)
        month = str(current_date.month)
        day = str(current_date.day)

        push_down_filter = 'IngestionYear = ' + year + ' and IngestionMonth = ' + month + ' and IngestionDay = ' + day

        accounts_raw_df = glueContext.create_dynamic_frame.from_catalog(
            database=database,
            table_name=table_name,
            push_down_predicate=push_down_filter
        ).toDF()
        anonymized_df = accounts_raw_df.where(col("isanonymized") == '1').select(col("id").alias("personid"))
        anonymized_df.repartition(1).write.mode("overwrite").parquet(s3_destination + '/year=' + year + '/month=' + month + '/day=' + day)
        store_last_updated(lastupdated_table, 'Anonymized_Persons', time.mktime(current_date.timetuple()))
        i = i + 1


if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, [
        'raw_db',
        'lastupdated_table',
        'raw_eol_contracts_table', 's3_eol_raw_contracts_destination',
        'raw_eol_accounts_table', 's3_eol_raw_accounts_destination',
        'raw_eol_paymentterms_table', 's3_eol_raw_paymentterms_destination',
        'raw_eol_persons_table', 's3_eol_raw_persons_email_destination',
        'raw_eol_users_table', 's3_eol_raw_users_googleclientid_destination',
        's3_domain_eol_accounts_hosting_view_destination',
        's3_eol_contracts_new_view_destination',
        's3_config_gdpr_personsdeletionlog_destination',
        's3_config_gdpr_accountsdeletionlog_destination'
    ])
    raw_db = args['raw_db']

    session = boto3.Session(region_name='eu-west-1')
    dynamodb = session.resource('dynamodb')
    lastupdated_table = dynamodb.Table(args['lastupdated_table'])

    load_contracts(raw_db, args['raw_eol_contracts_table'], args['s3_eol_raw_contracts_destination'], args['s3_eol_contracts_new_view_destination'])
    load_accounts(raw_db, args['raw_eol_accounts_table'], args['s3_eol_raw_accounts_destination'], args['s3_domain_eol_accounts_hosting_view_destination'])
    load_paymentterms(raw_db, args['raw_eol_paymentterms_table'], args['s3_eol_raw_paymentterms_destination'])
    load_persons_email(raw_db, args['raw_eol_persons_table'], args['s3_eol_raw_persons_email_destination'])
    load_users_googleclientid(raw_db, args['raw_eol_users_table'], args['s3_eol_raw_users_googleclientid_destination'])
    load_anonymized_accounts(raw_db, args['raw_eol_accounts_table'], args['s3_config_gdpr_accountsdeletionlog_destination'], lastupdated_table)
    load_anonymized_persons(raw_db, args['raw_eol_persons_table'], args['s3_config_gdpr_personsdeletionlog_destination'], lastupdated_table)