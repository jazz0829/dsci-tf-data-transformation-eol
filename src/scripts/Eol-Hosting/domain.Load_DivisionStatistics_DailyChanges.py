import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import when, col, row_number, lag, lead, lit, trim, upper
from pyspark.sql.session import SparkSession
from pyspark.sql.window import *
from datetime import date, timedelta
from pyspark.sql.types import DateType, LongType

args = getResolvedOptions(sys.argv, ['domain_db', 'divisionstatistics_daily_table', 's3_destination'])

spark = SparkSession.builder.getOrCreate()
glueContext = GlueContext(SparkContext.getOrCreate())

pre_trans = glueContext.create_dynamic_frame.from_catalog(database=args["domain_db"],
                                                          table_name=args["divisionstatistics_daily_table"]).toDF()

cols = ['environment', 'divisioncode', 'date', 'gltransactionscount', 'vattransactions', 'invoicelinescount',
        'sourcevatreturn', 'opportunitycount', 'accountcount', 'customercount', 'suppliercount', 'glaccountcount',
        'documentcount', 'itemcount', 'cashentrycount', 'cashtransactions', 'automaticbanklink', 'rabobankaccounts',
        'abnamrobankaccounts', 'ingbankaccounts', 'bankentrycount', 'banktransactions', 'banktransactionsimport',
        'banktransactionsmanual', 'salesentrycount', 'salestransactions', 'purchaseentrycount', 'purchasetransactions',
        'generaljournalentrycount', 'generaljournaltransactions', 'projecttotalemployees', 'projecttotaltimeentries',
        'projecttotalcostentries', 'projecttypefixedprice', 'projecttypetimeandmaterial', 'projecttypenonbillable',
        'projecttypeprepaidretainer', 'projecttypeprepaidhtb', 'salesorderentrycount', 'stockcountentrycount',
        'stockcounttransactions', 'salesinvoicecount', 'purchaseinvoicecount', 'starterusersaccesstocompanycount',
        'userswithchangedcompanyrightscount', 'userssetasinvolvedcount', 'customerswithinvolveduserscount',
        'leadsourcecount', 'electronicpurchaseentrycount', 'electronicsalesentrycount']

pre_trans = pre_trans.select(cols)
pre_trans = pre_trans.withColumn('environment',trim(upper(col('environment'))))
max_date = pre_trans.agg({'date':'max'}).collect()[0][0]
recent_df = pre_trans.where(col('date') == max_date)
old_df = pre_trans.where(col('date') < max_date)

old_df = old_df.withColumn("row_num", row_number().over(Window.partitionBy('environment', 'divisioncode').orderBy(col('date').desc())))
old_df = old_df.filter(col("row_num") == '1')

oldColumns = old_df.schema.names
newColumns = ['environment_older', 'divisioncode_older', 'date_older', 'gltransactionscount_older',
              'vattransactions_older', 'invoicelinescount_older', 'sourcevatreturn_older', 'opportunitycount_older',
              'accountcount_older', 'customercount_older', 'suppliercount_older', 'glaccountcount_older',
              'documentcount_older', 'itemcount_older', 'cashentrycount_older', 'cashtransactions_older',
              'automaticbanklink_older', 'rabobankaccounts_older', 'abnamrobankaccounts_older', 'ingbankaccounts_older',
              'bankentrycount_older', 'banktransactions_older', 'banktransactionsimport_older',
              'banktransactionsmanual_older', 'salesentrycount_older', 'salestransactions_older',
              'purchaseentrycount_older', 'purchasetransactions_older', 'generaljournalentrycount_older',
              'generaljournaltransactions_older', 'projecttotalemployees_older', 'projecttotaltimeentries_older',
              'projecttotalcostentries_older', 'projecttypefixedprice_older', 'projecttypetimeandmaterial_older',
              'projecttypenonbillable_older', 'projecttypeprepaidretainer_older', 'projecttypeprepaidhtb_older',
              'salesorderentrycount_older', 'stockcountentrycount_older', 'stockcounttransactions_older',
              'salesinvoicecount_older', 'purchaseinvoicecount_older', 'starterusersaccesstocompanycount_older',
              'userswithchangedcompanyrightscount_older', 'userssetasinvolvedcount_older',
              'customerswithinvolveduserscount_older', 'leadsourcecount_older', 'electronicpurchaseentrycount_older',
              'electronicsalesentrycount_older', 'row_older']

old_df = reduce(lambda old_df, idx: old_df.withColumnRenamed(oldColumns[idx], newColumns[idx]), xrange(len(oldColumns)),
               old_df)

oldColumns = recent_df.schema.names
newColumns = ['environment_recent', 'divisioncode_recent', 'date_recent', 'gltransactionscount_recent',
              'vattransactions_recent', 'invoicelinescount_recent', 'sourcevatreturn_recent', 'opportunitycount_recent',
              'accountcount_recent', 'customercount_recent', 'suppliercount_recent', 'glaccountcount_recent',
              'documentcount_recent', 'itemcount_recent', 'cashentrycount_recent', 'cashtransactions_recent',
              'automaticbanklink_recent', 'rabobankaccounts_recent', 'abnamrobankaccounts_recent',
              'ingbankaccounts_recent', 'bankentrycount_recent', 'banktransactions_recent',
              'banktransactionsimport_recent', 'banktransactionsmanual_recent', 'salesentrycount_recent',
              'salestransactions_recent', 'purchaseentrycount_recent', 'purchasetransactions_recent',
              'generaljournalentrycount_recent', 'generaljournaltransactions_recent', 'projecttotalemployees_recent',
              'projecttotaltimeentries_recent', 'projecttotalcostentries_recent', 'projecttypefixedprice_recent',
              'projecttypetimeandmaterial_recent', 'projecttypenonbillable_recent', 'projecttypeprepaidretainer_recent',
              'projecttypeprepaidhtb_recent', 'salesorderentrycount_recent', 'stockcountentrycount_recent',
              'stockcounttransactions_recent', 'salesinvoicecount_recent', 'purchaseinvoicecount_recent',
              'starterusersaccesstocompanycount_recent', 'userswithchangedcompanyrightscount_recent',
              'userssetasinvolvedcount_recent', 'customerswithinvolveduserscount_recent', 'leadsourcecount_recent',
              'electronicpurchaseentrycount_recent', 'electronicsalesentrycount_recent', 'row_recent']

recent_df = reduce(lambda recent_df, idx: recent_df.withColumnRenamed(oldColumns[idx], newColumns[idx]), xrange(len(oldColumns)),
                recent_df)

recent_df = recent_df.alias('recent')
old_df = old_df.alias('older')
cond = [recent_df.environment_recent == old_df.environment_older, recent_df.divisioncode_recent == old_df.divisioncode_older]
combined = recent_df.join(old_df, cond, 'left').select('recent.*', 'older.*')

count_cols = ['gltransactionscount', 'vattransactions', 'invoicelinescount', 'sourcevatreturn', 'opportunitycount',
              'accountcount', 'customercount', 'suppliercount', 'glaccountcount', 'documentcount', 'itemcount',
              'cashentrycount', 'cashtransactions', 'automaticbanklink', 'rabobankaccounts',
              'abnamrobankaccounts', 'ingbankaccounts', 'bankentrycount', 'banktransactionsimport', 'banktransactions',
              'banktransactionsmanual', 'salesentrycount', 'salestransactions', 'purchaseentrycount',
              'purchasetransactions',
              'generaljournalentrycount', 'generaljournaltransactions', 'projecttotalemployees',
              'projecttotaltimeentries',
              'projecttotalcostentries', 'projecttypefixedprice', 'projecttypetimeandmaterial',
              'projecttypenonbillable',
              'projecttypeprepaidretainer', 'projecttypeprepaidhtb', 'salesorderentrycount', 'stockcountentrycount',
              'stockcounttransactions', 'salesinvoicecount', 'purchaseinvoicecount', 'starterusersaccesstocompanycount',
              'userswithchangedcompanyrightscount', 'userssetasinvolvedcount', 'customerswithinvolveduserscount',
              'leadsourcecount', 'electronicpurchaseentrycount', 'electronicsalesentrycount']
for name in count_cols:
    combined = combined.withColumn(name,
                                   when(col(name + '_recent').isNull(), 0).otherwise(col(name + '_recent')) - when(
                                       col(name + '_older').isNull(), 0).otherwise(col(name + '_older')))

final = combined.select('environment_recent', 'divisioncode_recent', 'date_recent', 'gltransactionscount',
                        'vattransactions', 'invoicelinescount', 'sourcevatreturn', 'opportunitycount', 'accountcount',
                        'customercount', 'suppliercount', 'glaccountcount', 'documentcount', 'itemcount',
                        'cashentrycount',
                        'cashtransactions', 'automaticbanklink', 'rabobankaccounts', 'abnamrobankaccounts',
                        'ingbankaccounts', 'bankentrycount', 'banktransactions', 'banktransactionsimport',
                        'banktransactionsmanual', 'salesentrycount', 'salestransactions', 'purchaseentrycount',
                        'purchasetransactions', 'generaljournalentrycount', 'generaljournaltransactions',
                        'projecttotalemployees', 'projecttotaltimeentries', 'projecttotalcostentries',
                        'projecttypefixedprice', 'projecttypetimeandmaterial', 'projecttypenonbillable',
                        'projecttypeprepaidretainer', 'projecttypeprepaidhtb', 'salesorderentrycount',
                        'stockcountentrycount', 'stockcounttransactions', 'salesinvoicecount', 'purchaseinvoicecount',
                        'starterusersaccesstocompanycount', 'userswithchangedcompanyrightscount',
                        'userssetasinvolvedcount', 'customerswithinvolveduserscount', 'leadsourcecount',
                        'electronicpurchaseentrycount', 'electronicsalesentrycount')

final = final.withColumnRenamed('environment_recent', 'environment')
final = final.withColumnRenamed('divisioncode_recent', 'divisioncode')
final = final.withColumnRenamed('date_recent', 'date')

cols = ['divisioncode', 'gltransactionscount', 'vattransactions', 'invoicelinescount', 'sourcevatreturn',
        'opportunitycount', 'accountcount', 'customercount', 'suppliercount', 'glaccountcount', 'documentcount',
        'itemcount', 'cashentrycount', 'cashtransactions', 'automaticbanklink', 'rabobankaccounts',
        'abnamrobankaccounts', 'ingbankaccounts', 'bankentrycount', 'banktransactions', 'banktransactionsimport',
        'banktransactionsmanual', 'salesentrycount', 'salestransactions', 'purchaseentrycount', 'purchasetransactions',
        'generaljournalentrycount', 'generaljournaltransactions', 'projecttotalemployees', 'projecttotaltimeentries',
        'projecttotalcostentries', 'projecttypefixedprice', 'projecttypetimeandmaterial', 'projecttypenonbillable',
        'projecttypeprepaidretainer', 'projecttypeprepaidhtb', 'salesorderentrycount', 'stockcountentrycount',
        'stockcounttransactions', 'salesinvoicecount', 'purchaseinvoicecount', 'starterusersaccesstocompanycount',
        'userswithchangedcompanyrightscount', 'userssetasinvolvedcount', 'customerswithinvolveduserscount',
        'leadsourcecount', 'electronicpurchaseentrycount', 'electronicsalesentrycount']

for col_name in cols:
    final = final.withColumn(col_name, col(col_name).cast(LongType()))
    
final = final.drop('date')
final.repartition(1).write.mode("overwrite").parquet(args['s3_destination']+'/Date='+str(max_date))
