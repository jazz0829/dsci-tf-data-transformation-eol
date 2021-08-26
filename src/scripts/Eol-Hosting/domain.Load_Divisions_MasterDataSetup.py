import sys
import re
from utils import cleanDataFrame, copy_parquets
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import when, col, row_number, min, max, lit, sum, udf
from pyspark.sql.session import SparkSession
import datetime
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, TimestampType, StringType, BooleanType

def add_division_records(divisions_df, divisionstatistics_summary_df, accountscontract_summary_df, divisions_masterdatasetup_df):
    # ADD DIVISION RECORDS
    # First step adds Division records that have not already been added
    
    joined_df = divisions_df.join(accountscontract_summary_df, col("d.accountid") == col("acs.accountid"), "inner")\
                .join(divisionstatistics_summary_df, (col("d.environment") == col("dss.environment")) & (col("d.divisioncode") == col("dss.divisioncode")), "inner")\
                .join(divisions_masterdatasetup_df, (col("d.environment") == col("dmds.environment")) & (col("d.divisioncode") == col("dmds.divisioncode")), "left")\
                .withColumn('where_condition_value_1', when(col('acs.hadtrial') == 1, col('acs.firsttrialstartdate')).otherwise(col('acs.firstcommstartdate')))\
                .where(
                    ((col('where_condition_value_1') >= datetime.datetime(2017,1,1)) | (col('d.syscreated') >= datetime.datetime(2017,1,1)))
                    & col('dmds.divisioncode').isNull()
                )\
                .select(
                    'd.environment', 'd.divisioncode', 'd.accountid', 'acs.accountcode', 'dss.startuptype', 
                    'd.syscreated', 'dmds.divisionfirstaccessed', 'dmds.masterdatasetuptype', 'dmds.masterdatasetupstatus', 
                    'dmds.eventstarttime', 'dmds.eventlatesttime', 'dss.templatedivision')\
                .withColumn('divisionfirstaccessed', lit(None).cast(TimestampType()))\
                .withColumn('masterdatasetuptype', lit(None).cast(StringType()))\
                .withColumn('masterdatasetupstatus', lit(None).cast(StringType()))\
                .withColumn('eventstarttime', lit(None).cast(TimestampType()))\
                .withColumn('eventlatesttime', lit(None).cast(TimestampType()))\
                .withColumnRenamed('startuptype', 'divisionstartuptypecode')\
                .withColumnRenamed('syscreated', 'divisioncreated')\
                .withColumnRenamed('templatedivision', 'templatedivisioncode')\
                .withColumn('notes', lit(None).cast(StringType()))\
                .withColumn('templatedivisioncode', col("templatedivisioncode").cast(IntegerType()))\
                .select('d.environment', 'd.divisioncode', 'acs.accountcode', 'divisionstartuptypecode', 
                    'divisioncreated', 'templatedivisioncode', 'notes', 'd.accountid', 
                    'divisionfirstaccessed', 'eventstarttime', 'eventlatesttime', 'masterdatasetuptype', 'masterdatasetupstatus')
    
    divisions_masterdatasetup_df = divisions_masterdatasetup_df.select('environment', 'divisioncode', 'accountcode', 'divisionstartuptypecode', 
                    'divisioncreated', 'templatedivisioncode', 'notes', 'accountid', 
                    'divisionfirstaccessed', 'eventstarttime', 'eventlatesttime', 'masterdatasetuptype', 'masterdatasetupstatus')

    return divisions_masterdatasetup_df.union(joined_df)

def update_accountid_for_divisions(dmds_df, divisions_df):
    # UPDATE ACCOUNTID FOR DIVISIONS WHERE THIS HAS BEEN TRANSFERED 
    # This updates the AccountID for divisions that have been transfered from one Account to another.
    # This means we only get the 'latest' owner of a Division, as we also do with the domain.Divisions table.

    return dmds_df.alias("dmds")\
                .join(divisions_df, (col("d.environment") == col("dmds.environment")) & (col("d.divisioncode") == col("dmds.divisioncode")), "inner")\
                .withColumn('accountid_', when(col('dmds.accountid') != col('d.accountid'), col('d.accountid')).otherwise(col('dmds.accountid')))\
                .drop("accountid")\
                .select("dmds.*", col("accountid_").alias("accountid"))

def update_division_firstaccessed(dmds_df, activitydaily_df):
    # DIVISION FIRST ACCESSED
    # This updates each division with the date/timestamp we have for the first record in the EOLActivityLog for that AccountID for that division
    # This timestamp is used in place of the syscreated fields for Divisions or DivisionStatistics_Summary because these can be created based on the record
    # being generated as part of a division pool (in the case of a template) rather than when the division was created for the customer. In the case of a 
    # conversion the syscreated field can be when the division was first created in a different system rather than when it was availabled in EOL.
    # NOTE: this is currently not being updated when the AccountID changes. So, for example, if a division is created first by an accountant and then transferred to an 
    #		 entrepreneur the DivisionFirstAccessed time will stay at the time the Accountant first accessed the division. 

    joined_df = dmds_df.alias("dmds")\
            .join(activitydaily_df, 
                    (col("ad.accountid") == col("dmds.accountid")) & 
                    (col("ad.divisioncode") == col("dmds.divisioncode")) & 
                    (col("ad.date") >= col("dmds.divisioncreated")) &
                    col("dmds.divisionfirstaccessed").isNull()
            , "inner")\
            .where(col("ad.activityid") == '1') \
            .groupBy("dmds.environment", "dmds.accountid", "dmds.divisioncode").agg(min("ad.date").alias("firstlogdatetime"))\
            .alias("al")

    return dmds_df.alias("dmds")\
                .join(joined_df, 
                    (col("al.accountid") == col("dmds.accountid")) & 
                    (col("al.divisioncode") == col("dmds.divisioncode"))
                    , "left")\
                .withColumn("divisionfirstaccessed", col("al.firstlogdatetime").cast(TimestampType()))\
                .select("dmds.*", "divisionfirstaccessed")

def temporary_divisions(dmds_df, divisions_raw_df):
    # TEMPORARY DIVISIONS
    # Updates the table with divisions that were created and deleted (scheduled for deletion) on the same day that we get Hosting data for.

    divisions_raw_df = divisions_raw_df\
                        .withColumn('code', col('code').cast(TimestampType()))\
                        .withColumn('cigcopytime', col('cigcopytime').cast(TimestampType()))
    
    window_spec = Window.partitionBy(col('environment'), col('code')).orderBy(col('cigcopytime').desc())
    divisions_raw_df = divisions_raw_df\
                        .withColumn("row_num", row_number().over(window_spec))\
                        .withColumn('divisioncode', col('code').cast(IntegerType()))\
                        .withColumn('blockingstatuscode', col('blockingstatus').cast(IntegerType()))\
                        .where(col('row_num') == 1)\
                        .select('environment', 'divisioncode', 'blockingstatuscode')\
                        .alias('d')

    return dmds_df.alias("dmds")\
                .join(divisions_raw_df, (col("d.environment") == col("dmds.environment")) & (col("d.divisioncode") == col("dmds.divisioncode")), "left")\
                .withColumn('masterdatasetuptype_', 
                    when(col('dmds.masterdatasetuptype').isNull() & (col('d.blockingstatuscode') >= 100), 
                    lit('TemporaryDivision')).otherwise(col('dmds.masterdatasetuptype'))
                )\
                .drop("masterdatasetuptype")\
                .select("dmds.*", col("masterdatasetuptype_").alias("masterdatasetuptype"))

def demo_template(dmds_df):
    # DEMO TEMPLATE
    # When a division has the Y startup type this is a demo company template used for trial purposes. 
    # This template contains prefilled master data and some existing transactions. This type of template should not be used for actual financial administration.
    return dmds_df.withColumn('masterdatasetuptype', 
                    when(
                        col('masterdatasetuptype').isNull() & (col('divisionstartuptypecode') == "Y"), 
                        lit('Demo')
                    ).otherwise(col('masterdatasetuptype'))
                )

def conversion(dmds_df, conversionsource_df, activitydaily_df, divisions_df):
    # CONVERSION
    # This updates the divisions master data setup type based on Conversions from the ConversionSource view.

    joined_df = conversionsource_df\
                    .join(divisions_df, (col("d.environment") == col("cs.environment")) & (col("d.divisioncode") == col("cs.divisioncode")), "inner")\
                    .join(
                            activitydaily_df\
                                .where(col("activityid").isin(
                                    '840000', # EOL Conversion Tool Cubic Dos
                                    '840100', # EOL Conversion Tool for DOS
                                    '840200', # EOL Conversion Tool for Exact for Windows
                                    '840300', # EOL Conversion Tool
                                    '840400'  # TwinfieldConversion
                                ))\
                                .groupBy("environment", "divisioncode").agg(min("ad.date").alias("firstconvtime"))\
                                .select("environment", "divisioncode", "firstconvtime")\
                                .alias("fc"),
                            (col("fc.environment") == col("cs.environment")) & (col("fc.divisioncode") == col("cs.divisioncode")), 
                            "left"
                        )\
                    .select(
                        "cs.environment", "cs.divisioncode", "cs.conversionsource", "cs.conversionstatuscode", "cs.conversionstatusdescription", 
                        "cs.syscreated", "cs.sysmodified", "d.linkeddivision", "fc.firstconvtime"
                    )\
                    .alias("convsq")

    where_statement = udf(
        lambda syscreated, masterdatasetuptype, masterdatasetupstatus: 
            syscreated is not None and (masterdatasetuptype is None or (masterdatasetuptype == "Conversion" and masterdatasetupstatus not in ['Success', 'Failure'])),
        BooleanType()
    )

    dmds_df = dmds_df.alias("dmds")\
                .join(joined_df, (col("convsq.environment") == col("dmds.environment")) & (col("convsq.linkeddivision") == col("dmds.divisioncode")), "left")\
                .withColumn("masterdatasetuptype_", 
                    when(where_statement(col('convsq.syscreated'), col('dmds.masterdatasetuptype'), col('dmds.masterdatasetupstatus')), lit('Conversion'))\
                    .otherwise(col("dmds.masterdatasetuptype"))
                )\
                .withColumn("masterdatasetupstatus_", 
                    when(where_statement(col('convsq.syscreated'), col('dmds.masterdatasetuptype'), col('dmds.masterdatasetupstatus')), 
                        when(col('convsq.conversionstatusdescription') == "csHandedOver", lit('Success'))\
                        .when(col('convsq.conversionstatusdescription') == "csHandoverFailed", lit('Failure'))
                        .otherwise(lit('InProgress'))
                    )\
                    .otherwise(col("dmds.masterdatasetupstatus"))
                )\
                .drop("masterdatasetupstatus", "masterdatasetuptype")\
                .select("dmds.*", col("masterdatasetuptype_").alias("masterdatasetuptype"), col("masterdatasetupstatus_").alias("masterdatasetupstatus"))
    # This step then deletes the division rows of the 'holding' divisions created as part of the conversion process
    return dmds_df.alias("dmds")\
                .join(conversionsource_df, (col("cs.environment") == col("dmds.environment")) & (col("cs.divisioncode") == col("dmds.divisioncode")), "left_anti")\
                .select("dmds.*")

def get_division_code(value):
    if not isinstance(value, basestring):
        return None
    number_first_match = re.search(r"[0-9]", value)
    if number_first_match == None:
        return None
    number_first_match = number_first_match.start()
    return int(value[number_first_match:][:value[number_first_match:].index(':')].strip())

def division_transfer(dmds_df, requests_transferrequest_df, requests_executetransferrequest_df, contacts_df, users_df):
    # DIVISION TRANSFER
    # This updates the divisions master data setup type based on a division transfer from another customer account.
    # This transfer can occur when a division has moved from an accountant to an entrepreneur, but also a transfer between entrepreneur accounts (often the same customer running multiple accounts).

    get_division_code_udf = udf(
        lambda value: get_division_code(value),
        IntegerType()
    )

    window_spec = Window.partitionBy(col('tr.environment'), col('divisioncode')).orderBy(col('tr.created'), col('tr.realized'), col('etr.created'), col('etr.realized'), col('tr.status').desc())

    joined_df = requests_transferrequest_df\
                    .join(requests_executetransferrequest_df, col("tr.requestid") == col("etr.relatedrequest"), "left")\
                    .join(contacts_df, col("tr.contactid") == col("c.contactid"), "left")\
                    .join(users_df, col("c.personid") == col("u.personid"), "left")\
                    .withColumn("divisioncode", get_division_code_udf(col("etr.requestcomments")))\
                    .withColumn("row_num", row_number().over(window_spec))\
                    .select("tr.environment", "divisioncode", col("tr.created").alias("tr_created"), col("tr.realized").alias("tr_realized"), "tr.status", "tr.typedescription", "etr.description", "etr.created", "etr.realized")\
                    .where(col("row_num") == 1)\
                    .alias("treq")

    where_statement = udf(
        lambda tr_created, masterdatasetuptype, masterdatasetupstatus: 
            tr_created is not None and (masterdatasetuptype is None or (masterdatasetuptype == "DivisionTransfer" and masterdatasetupstatus not in ['Success', 'Failure', 'Rejected'])),
        BooleanType()
    )

    return dmds_df.alias("dmds")\
                .join(joined_df, (col("treq.environment") == col("dmds.environment")) & (col("treq.divisioncode") == col("dmds.divisioncode")), "left")\
                .withColumn("masterdatasetuptype_", 
                    when(
                        where_statement(col('treq.tr_created'), col('dmds.masterdatasetuptype'), col('dmds.masterdatasetupstatus')), 
                        lit('DivisionTransfer')
                    )\
                    .otherwise(col("dmds.masterdatasetuptype"))
                )\
                .withColumn("masterdatasetupstatus_", 
                    when(where_statement(col('treq.tr_created'), col('dmds.masterdatasetuptype'), col('dmds.masterdatasetupstatus')),
                         when(col('treq.description') == "Request To Transfer", lit('Success'))\
                        .when(col('treq.description') == "Error on Transfer", lit('Failure'))\
                        .when(col('treq.status') == 5, lit('Rejected'))\
                        .when(col('treq.description').isNull(), lit('In Progress'))
                    )
                    .otherwise(col("dmds.masterdatasetupstatus"))
                )\
                .withColumn("eventstarttime", 
                    when(
                        where_statement(col('treq.tr_created'), col('dmds.masterdatasetuptype'), col('dmds.masterdatasetupstatus')), 
                        col("treq.created")
                    )\
                    .otherwise(col("dmds.eventstarttime"))
                )\
                .withColumn("eventlatesttime", 
                    when(
                        where_statement(col('treq.tr_created'), col('dmds.masterdatasetuptype'), col('dmds.masterdatasetupstatus')), 
                        when(col('treq.realized').isNotNull(), col('treq.realized'))\
                        .when(col('treq.status') == 5, col('treq.tr_created'))
                    )\
                    .otherwise(col("dmds.eventlatesttime"))
                )\
                .drop("masterdatasetupstatus", "masterdatasetuptype")\
                .select("dmds.*", col("masterdatasetuptype_").alias("masterdatasetuptype"), col("masterdatasetupstatus_").alias("masterdatasetupstatus"), "eventstarttime", "eventlatesttime")

def update_divisions_xmlcsv_import_attempted(dmds_df, activitydaily_df):
    # RUNS THE UPDATE FOR DIVISIONS THAT ATTEMPTED A XML/CSV IMPORT OF GL ACCOUNTS
    # THIS LOGIC CAN BE IMPROVED IN A NUMBER OF WAYS: 
    #  - NOT ONLY LOOKING AT XML/CSV IMPORT BUT OTHER IMPORT BEHAVIORS
    #  CURRENTLY THIS PART OF THE CODE ALSO RUNS AFTER THE EXACT TEMPLATE AND EMPTY SECTIONS OF THE CODE. THIS MEANS THAT THE MasterDataSetupStatus
    #  IS ALSO APPEARING FOR RECORDS THAT FIRST MEET THIS SETUP TYPE CATEGORY - THIS NEEDS TO BE FIXED


    joined_df = dmds_df.alias("dmds")\
                    .join(activitydaily_df.alias("al"), ((col("dmds.accountid") == col("al.accountid")) & (col("dmds.divisioncode") == col("al.divisioncode"))), "inner")\
                    .where(col("al.activityid").isin('5304', '5335', '5304202', '5334202') & (col("al.date") >= col("dmds.divisioncreated")))\
                    .withColumn("eventlasttime", when(col("al.activityid").isin('5304202', '5334202'), col("al.date")))\
                    .groupBy("dmds.environment", "dmds.accountid", "al.divisioncode").agg(min("al.date").alias("eventstarttime"), max("eventlasttime").alias("eventlasttime"))\
                    .alias("xml")

    where_statement = udf(
        lambda eventstarttime, eventlasttime, masterdatasetuptype, masterdatasetupstatus: 
            (eventstarttime is not None or eventlasttime is not None) and (masterdatasetuptype is None or 
            (masterdatasetuptype == "XML/CSV_Import" and masterdatasetupstatus != "Success") or 
            masterdatasetuptype == "ExactTemplate" or masterdatasetuptype == "Empty"),
        BooleanType()
    )

    return dmds_df.alias("dmds")\
                .join(joined_df, (col("xml.environment") == col("dmds.environment")) & (col("xml.divisioncode") == col("dmds.divisioncode")), "left")\
                .withColumn("masterdatasetuptype_", 
                    when(
                        where_statement(col("xml.eventstarttime"), col("xml.eventlasttime"), col('dmds.masterdatasetuptype'), col('dmds.masterdatasetupstatus')), 
                        lit('XML/CSV_Import')
                    )\
                    .otherwise(col("dmds.masterdatasetuptype"))
                )\
                .withColumn("masterdatasetupstatus_", 
                    when(
                        where_statement(col("xml.eventstarttime"), col("xml.eventlasttime"), col('dmds.masterdatasetuptype'), col('dmds.masterdatasetupstatus')),
                        when(col('xml.eventlasttime').isNotNull(), lit('Success')).otherwise("Started")
                    )\
                    .otherwise(col("dmds.masterdatasetupstatus"))
                )\
                .withColumn("eventstarttime_", 
                    when(
                        where_statement(col("xml.eventstarttime"), col("xml.eventlasttime"), col('dmds.masterdatasetuptype'), col('dmds.masterdatasetupstatus')), 
                        col("xml.eventstarttime")
                    )\
                    .otherwise(col("dmds.eventstarttime"))
                )\
                .withColumn("eventlatesttime_", 
                    when(
                        where_statement(col("xml.eventstarttime"), col("xml.eventlasttime"), col('dmds.masterdatasetuptype'), col('dmds.masterdatasetupstatus')),
                        col("xml.eventlasttime")
                    )\
                    .otherwise(col("dmds.eventlatesttime"))
                )\
                .drop("masterdatasetuptype", "masterdatasetupstatus", "eventstarttime", "eventlatesttime")\
                .select("dmds.*", 
                    col("masterdatasetuptype_").alias("masterdatasetuptype"), 
                    col("masterdatasetupstatus_").alias("masterdatasetupstatus"), 
                    col("eventstarttime_").alias("eventstarttime"), 
                    col("eventlatesttime_").alias("eventlatesttime")
                )
                # NEED TO ADD LOGIC TO LOOK AT TEMPLATES TOO!!!!!!!! ALSO ACCOUNTANT TEMPLATE

def exact_template(dmds_df, activitydaily_df):
    # EXACT TEMPLATE
    # For the Exact Legislation template a division starts with approximately 124 GL Accounts already created. This enables the user to have a starting base of financial master data.

    window_spec_1 = Window.partitionBy(col('accountcode'), col('divisioncode')).orderBy(col('date').asc())
    window_spec_2 = Window.partitionBy(col('environment'), col('accountcode'), col('divisioncode'), col('savemoment')).orderBy(col('date').asc())
    joined_df = activitydaily_df.withColumn("savemoment", sum('quantity').over(window_spec_1))\
                            .where(col('activityid') == '511006')\
                            .withColumn('savemoment', 
                                when(col('savemoment') <= 5, lit(1))\
                                .when(col('savemoment') >= 5, lit(5))\
                                .otherwise(None)
                            )\
                            .select("date", "accountcode", "divisioncode", "environment", "savemoment")\
                            .withColumn("row_num", row_number().over(window_spec_2))\
                            .withColumn('firstsave', when(col('savemoment').isin(1,5), col('date')).otherwise(None))\
                            .withColumn('fifthsave', when(col('savemoment') == 5, col('date')).otherwise(None))\
                            .where(col("row_num") == 1)\
                            .select("date", "accountcode", "divisioncode", "environment", "firstsave", "fifthsave")\
                            .alias("saveactions")

    where_statement = udf(
        lambda masterdatasetuptype, masterdatasetupstatus, divisionstartuptypecode: 
            (masterdatasetuptype is None or 
            (masterdatasetuptype == "XML/CSV_Import" and masterdatasetupstatus != "Success")) and
            divisionstartuptypecode == "X",
        BooleanType()
    )

    return dmds_df.alias("dmds")\
                .join(joined_df, 
                        (col("saveactions.environment") == col("dmds.environment")) & 
                        (col("saveactions.divisioncode") == col("dmds.divisioncode")) & 
                        (col("saveactions.accountcode") == col("dmds.accountcode")),
                    "left")\
                .withColumn("masterdatasetuptype_", 
                     when(
                        where_statement(col('dmds.masterdatasetuptype'), col('dmds.masterdatasetupstatus'), col("dmds.divisionstartuptypecode")), 
                        lit('ExactTemplate')
                    )\
                    .otherwise(col("dmds.masterdatasetuptype"))
                )\
                .withColumn("masterdatasetupstatus_", 
                    when(
                        where_statement(col('dmds.masterdatasetuptype'), col('dmds.masterdatasetupstatus'), col("dmds.divisionstartuptypecode")), 
                        when(col('saveactions.fifthsave').isNotNull(), lit('GLChangesMade'))\
                        .when(col('saveactions.firstsave').isNotNull() & col('saveactions.fifthsave').isNull(), lit('GLChangesStarted'))\
                        .otherwise(lit("StandardTemplate"))
                    )\
                    .otherwise(col("dmds.masterdatasetupstatus"))
                )\
                .drop("masterdatasetuptype", "masterdatasetupstatus")\
                .select("dmds.*", 
                    col("masterdatasetuptype_").alias("masterdatasetuptype"), 
                    col("masterdatasetupstatus_").alias("masterdatasetupstatus")
                )

def accountant_template(dmds_df):
    # ACCOUNTANT TEMPLATE
    # When a division is started based on an Accountant template the assumption is that the division already has the master data deemed appropriate by the
    # accountant available to the user.

    return dmds_df.withColumn("masterdatasetuptype", 
        when(col("masterdatasetuptype").isNull() & (col("divisionstartuptypecode") == "A"), lit('AccountantTemplate'))\
        .otherwise(col("masterdatasetuptype"))
    )


def division_copy(dmds_df, divisionstatistics_summary_df):
    return dmds_df.alias("dmds")\
                .join(
                    divisionstatistics_summary_df, 
                    (col("dss.environment") == col("dmds.environment")) & 
                    (col("dss.divisioncode") == col("dmds.divisioncode")), "left"
                )\
                .withColumn("masterdatasetuptype_", 
                    when(col("dmds.masterdatasetuptype").isNull() & (col("dmds.divisionstartuptypecode") == "D"), lit('DivisionCopy'))\
                    .otherwise(col("dmds.masterdatasetuptype"))
                )\
                .drop("masterdatasetuptype")\
                .select("dmds.*", col("masterdatasetuptype_").alias("masterdatasetuptype"))

def empty_division(dmds_df, activitydaily_df):
    # EMPTY DIVISION
    # A division can start without any master data. This is not uncommon to see when a customer starts 

    window_spec_1 = Window.partitionBy(col('accountcode'), col('divisioncode')).orderBy(col('date').asc())
    window_spec_2 = Window.partitionBy(col('environment'), col('accountcode'), col('divisioncode'), col('savemoment')).orderBy(col('date').asc())
    joined_df = activitydaily_df.withColumn("savemoment", sum('quantity').over(window_spec_1))\
                            .where(col('activityid') == '511006')\
                            .withColumn('savemoment', 
                                when(col('savemoment') <= 5, lit(1))\
                                .when(col('savemoment') >= 5, lit(5))\
                                .otherwise(None)
                            )\
                            .select("date", "accountcode", "divisioncode", "environment", "savemoment")\
                            .withColumn("row_num", row_number().over(window_spec_2))\
                            .withColumn('firstsave', when(col('savemoment').isin(1,5), col('date')).otherwise(None))\
                            .withColumn('fifthsave', when(col('savemoment') == 5, col('date')).otherwise(None))\
                            .where(col("row_num") == 1)\
                            .select("date", "accountcode", "divisioncode", "environment", "firstsave", "fifthsave")\
                            .alias("saveactions")

    where_statement = udf(
        lambda masterdatasetuptype, divisionstartuptypecode: 
            masterdatasetuptype is None and divisionstartuptypecode == "E",
        BooleanType()
    )

    return dmds_df.alias("dmds")\
                .join(joined_df, 
                        (col("saveactions.environment") == col("dmds.environment")) & 
                        (col("saveactions.divisioncode") == col("dmds.divisioncode")) & 
                        (col("saveactions.accountcode") == col("dmds.accountcode")),
                    "left")\
                .withColumn("masterdatasetuptype_", 
                    when(
                        where_statement(col('dmds.masterdatasetuptype'), col("dmds.divisionstartuptypecode")), 
                        lit('Empty')
                    )\
                    .otherwise(col("dmds.masterdatasetuptype"))
                )\
                .withColumn("masterdatasetupstatus_", 
                    when(
                        where_statement(col('dmds.masterdatasetuptype'), col("dmds.divisionstartuptypecode")), 
                        when(col('saveactions.fifthsave').isNotNull(), lit('GLAccountsAdded'))\
                        .when(col('saveactions.firstsave').isNotNull() & col('saveactions.fifthsave').isNull(), lit('GLAccountsStarted'))\
                        .otherwise(lit("NoChanges"))
                    )\
                    .otherwise(col("dmds.masterdatasetupstatus"))
                )\
                .drop("masterdatasetuptype", "masterdatasetupstatus")\
                .select("dmds.*", 
                    col("masterdatasetuptype_").alias("masterdatasetuptype"), 
                    col("masterdatasetupstatus_").alias("masterdatasetupstatus")
                )

if __name__ == "__main__":
    glueContext = GlueContext(SparkContext.getOrCreate())

    args = getResolvedOptions(sys.argv, [
        'raw_db',
        'domain_db',
        'raw_eol_divisions_table',
        'domain_divisions_table',
        'domain_accountscontract_summary_table',
        'domain_divisionstatistics_summary_table',
        'domain_activitydaily_table',
        'domain_divisions_masterdatasetup_table',
        'domain_requests_transferrequest_table',
        'domain_requests_executetransferrequest_table',
        'domain_contacts_table',
        'domain_users_table',
        'domain_conversionsource_table',
        's3_bucket',
        's3_destination'
    ])
    raw_db = args['raw_db']
    domain_db = args['domain_db']
    divisions_raw_df = glueContext.create_dynamic_frame.from_catalog(raw_db, args["raw_eol_divisions_table"]).toDF()

    divisions_df = glueContext.create_dynamic_frame.from_catalog(domain_db, args["domain_divisions_table"]).toDF()
    accountscontract_summary_df = glueContext.create_dynamic_frame.from_catalog(domain_db, args["domain_accountscontract_summary_table"]).toDF().alias("acs")
    divisionstatistics_summary_df = glueContext.create_dynamic_frame.from_catalog(domain_db, args["domain_divisionstatistics_summary_table"]).toDF()
    activitydaily_df = glueContext.create_dynamic_frame.from_catalog(domain_db, args["domain_activitydaily_table"]).toDF()\
                                .withColumn('divisioncode', col("divisioncode").cast(IntegerType()))
    dmds_df = glueContext.create_dynamic_frame.from_catalog(domain_db, args["domain_divisions_masterdatasetup_table"]).toDF()
    requests_transferrequest_df = glueContext.create_dynamic_frame.from_catalog(domain_db, args["domain_requests_transferrequest_table"]).toDF()
    requests_executetransferrequest_df = glueContext.create_dynamic_frame.from_catalog(domain_db, args["domain_requests_executetransferrequest_table"]).toDF().alias("etr")
    contacts_df = glueContext.create_dynamic_frame.from_catalog(domain_db, args["domain_contacts_table"]).toDF().alias("c")
    users_df = glueContext.create_dynamic_frame.from_catalog(domain_db, args["domain_users_table"]).toDF().alias("u")
    conversionsource_df = glueContext.create_dynamic_frame.from_catalog(domain_db, args["domain_conversionsource_table"]).toDF().alias("cs")
    
    divisions_raw_df = cleanDataFrame(divisions_raw_df, to_upper_cols=['environment'])

    divisionstatistics_summary_df = cleanDataFrame(divisionstatistics_summary_df, to_upper_cols=['environment']).alias("dss")
    divisions_df = cleanDataFrame(divisions_df, to_upper_cols=['environment']).alias("d")
    requests_transferrequest_df = cleanDataFrame(requests_transferrequest_df, to_upper_cols=['environment']).alias("tr")
    activitydaily_df = cleanDataFrame(activitydaily_df, to_upper_cols=['environment']).alias("ad")
    dmds_df = cleanDataFrame(dmds_df, to_upper_cols=['environment']).alias("dmds")

    dmds_df = add_division_records(divisions_df, divisionstatistics_summary_df, accountscontract_summary_df, dmds_df)
    dmds_df = update_accountid_for_divisions(dmds_df, divisions_df)
    dmds_df = update_division_firstaccessed(dmds_df, activitydaily_df)
    dmds_df = temporary_divisions(dmds_df, divisions_raw_df)
    dmds_df = demo_template(dmds_df)
    dmds_df = conversion(dmds_df, conversionsource_df, activitydaily_df, divisions_df)
    dmds_df = division_transfer(dmds_df, requests_transferrequest_df, requests_executetransferrequest_df, contacts_df, users_df)
    dmds_df = update_divisions_xmlcsv_import_attempted(dmds_df, activitydaily_df)
    dmds_df = exact_template(dmds_df, activitydaily_df)
    dmds_df = accountant_template(dmds_df)
    dmds_df = division_copy(dmds_df, divisionstatistics_summary_df)
    dmds_df = empty_division(dmds_df, activitydaily_df)

    dmds_df.repartition(2).write.mode("overwrite").parquet(args["s3_destination"]+"_Temp")
    copy_parquets(args["s3_bucket"], "Data/Divisions_MasterDataSetup_Temp", "Data/Divisions_MasterDataSetup")