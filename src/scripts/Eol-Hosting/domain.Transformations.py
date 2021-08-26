import sys
from utils import *
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, trim, upper, concat, substring, lpad, lit, when, udf,min
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()
glueContext = GlueContext(SparkContext.getOrCreate())

# Covered End to End
def load_oauthclients_view(database, table_name, s3_destination):
    oauthclients = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()
    oauthclients = oauthclients.withColumn("row_num", row_number().over(Window.partitionBy("id").orderBy(col("cigcopytime").desc())))
    oauthclients = oauthclients.filter(col("row_num") == '1')
    oauthclients = oauthclients.withColumnRenamed("id", "applicationID")
    oauthclients = oauthclients.drop(col("row_num"))
    selectcols = ['applicationid', 'account', 'descriptiontermid', 'description', 'applicationname', 'logo',
                  'type', 'startdate', 'enddate', 'returnurl', 'syscreated', 'syscreator', 'sysmodified',
                  'sysmodifier', 'logofilename', 'privilegedaccess', 'publishdate', 'allowresourceownerFlow',
                  'category', 'cigcopytime', 'environment', 'cigprocessed']
    oauthclients = oauthclients.select(selectcols)

    oauthclients.repartition(1).write.mode("overwrite").parquet(s3_destination)


def load_oauthclientslog_view(database, table_name, s3_destination):
    oauthclientslog = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()
    oauthclientslog = oauthclientslog.groupBy("publishdate", "id").agg(min('sysmodified'))
    oauthclientslog = oauthclientslog.withColumnRenamed('min(sysmodified)','sysmodified')
    oauthclientslog = oauthclientslog.withColumn("publishdate", when(col('publishdate').isNull(),
                                                                     col('sysmodified').cast(DateType())).otherwise(
        col('publishdate'))) \
        .withColumn("event", when(col('publishdate').isNull(), lit('Unpublished')).otherwise(lit('Published')))
    oauthclientslog = oauthclientslog.drop(col('sysmodified'))
    oauthclientslog = oauthclientslog.withColumnRenamed("id","oauthclientid")

    oauthclientslog.repartition(1).write.mode("overwrite").parquet(s3_destination)

# Covered End to End
def load_users(database, table_name, s3_destination):
    users_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()
    users_raw_df = cleanDataFrame(users_raw_df, ['id','environment'])
    users_raw_df = convertDataTypes(
        data_frame = users_raw_df,
        integer_cols= ['hid', 'authenticationtype', 'lockoutattempts'],
        boolean_cols = ['blocked'],
        timestamp_cols=[
            'cigcopytime', 'syscreated', 'sysmodified', 'lockouttime', 'startdate', 'enddate', 
            'validationdate', 'lastlogin', 'totpregistrationutcdate', 'totpskippedutc', 
            'totpforcedbyexactutc', 'marketingoptindate', 'marketingoptoutdate'
        ]
    )
    window_spec = Window.partitionBy(col('id')).orderBy(col('cigcopytime').desc())
    users_df = users_raw_df.withColumn("row_num", row_number().over(window_spec))

    users_df = renameColumns(users_df, {
        'id': 'userid',
        'customer': 'accountid',
        'person': 'personid'
    })
    the_latest_record_index = 1
    users_df = users_df.where(col('row_num') == the_latest_record_index)
    users_df = users_df.drop('row_num', 'ingestionday', 'ingestionmonth', 'ingestionyear', 'googleclientid', 'username', 'cigprocessed')
    users_df.repartition(2).write.mode("overwrite").parquet(s3_destination)

def load_persons(database, table_name, s3_destination):
    persons_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()
    persons_raw_df = cleanDataFrame(persons_raw_df, ['id','environment'])
    persons_raw_df = convertDataTypes(
        data_frame = persons_raw_df,
        integer_cols= ['hid','namecomposition','anonymisationsource'],
        boolean_cols = ['isanonymized'],
        timestamp_cols=['cigcopytime', 'syscreated', 'sysmodified']
    )
    window_spec = Window.partitionBy(col('id')).orderBy(col('cigcopytime').desc())
    persons_df = persons_raw_df.withColumn("row_num", row_number().over(window_spec))

    persons_df = renameColumns(persons_df, {
        'id': 'personid',
        'customer': 'accountid'
    })
    the_latest_record_index = 1
    persons_df = persons_df.where(col('row_num') == the_latest_record_index)
    persons_df = persons_df.drop('row_num', 'ingestionday', 'ingestionmonth', 'hid', 'ingestionyear', 'googleclientid', 'username', 'cigprocessed', 'longitude', 'latitude', 'addressline3', 'email')
    persons_df.repartition(2).write.mode("overwrite").parquet(s3_destination)

# Covered End to End
def load_activitysectors(database, table_name, s3_destination):
    activitysectors_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()
    activitysectors_raw_df = cleanDataFrame(activitysectors_raw_df, ['id','environment'])
    activitysectors_raw_df = convertDataTypes(
        data_frame = activitysectors_raw_df,
        integer_cols = ['descriptiontermid'],
        timestamp_cols = ['syscreated', 'sysmodified', 'cigcopytime'],
        boolean_cols = ['cigprocessed']
    )
    window_spec = Window.partitionBy(col('id')).orderBy(col('cigcopytime').desc())
    activitysectors_df = activitysectors_raw_df.withColumn("row_num", row_number().over(window_spec))
    the_latest_record_index = 1
    activitysectors_df = activitysectors_df.where(col('row_num') == the_latest_record_index)
    activitysectors_df = activitysectors_df.drop('row_num', 'ingestionday', 'ingestionmonth', 'ingestionyear')
    activitysectors_df.repartition(1).write.mode("overwrite").parquet(s3_destination)

# Covered End to End
def load_companysizes(database, table_name, s3_destination):
    companysize_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name = table_name).toDF()
    companysize_raw_df = cleanDataFrame(companysize_raw_df, ['id','environment'])
    companysize_raw_df = convertDataTypes(
        data_frame = companysize_raw_df,
        integer_cols = ['descriptiontermid', 'employeecountfrom', 'employeecountto'],
        timestamp_cols = ['syscreated', 'sysmodified', 'cigcopytime'],
        boolean_cols = ['cigprocessed']
    )
    window_spec = Window.partitionBy(col('id')).orderBy(col('cigcopytime').desc())
    companysize_df = companysize_raw_df.withColumn("row_num", row_number().over(window_spec))
    the_latest_record_index = 1
    companysize_df = companysize_df.where(col('row_num') == the_latest_record_index)
    companysize_df = companysize_df.drop('row_num', 'ingestionday', 'ingestionmonth', 'ingestionyear')

    companysize_df.repartition(1).write.mode("overwrite").parquet(s3_destination)

def load_contacts(database, table_name, s3_destination):
    contacts_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()
    contacts_raw_df = cleanDataFrame(contacts_raw_df, ['id','environment'])
    contacts_raw_df = convertDataTypes(
        data_frame = contacts_raw_df,
        integer_cols = ['division'],
        timestamp_cols = ['syscreated', 'sysmodified', 'startdate', 'enddate', 'cigcopytime'],
        boolean_cols = ['ismailingexcluded', 'isuxparticipant']
    )
    
    window_spec = Window.partitionBy(col('id')).orderBy(col('cigcopytime').desc())
    contacts_df = contacts_raw_df.withColumn("row_num", row_number().over(window_spec))

    contacts_df = renameColumns(contacts_df, {
        'id': 'contactid',
        'person': 'personid',
        'division': 'divisioncode',
        'account': 'accountid',
        "businesstypeid": "businesstype",
        "companysizeid": "companysize",
        "sectorid": "activitysector",
        "subsectorid": "activitysubsector"
    })

    contacts_df = contacts_df.where(col('row_num') == 1)
    contacts_df = contacts_df.drop('row_num', 'ingestionday', 'ingestionmonth', 'ingestionyear', 'cigprocessed')

    contacts_df.repartition(1).write.mode("overwrite").parquet(s3_destination)

# Covered End to End
def load_divisions(database, table_name, s3_destination):
    divisions_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()

    divisions_raw_df = convertDataTypes(
        data_frame = divisions_raw_df,
        boolean_cols=['main', 'customerportal'],
        timestamp_cols=['cigcopytime', 'syscreated', 'sysmodified', 'deleted', 'enddate', 'startdate'],
        double_cols=['sharecapital'],
        integer_cols= ['code', 'blockingstatus', 'status', 'linkeddivision', 'origin'],
        long_cols = ['legislationcode']
    )
    divisions_raw_df = divisions_raw_df.withColumn('environment', upper(trim(col('environment'))))
    window_spec = Window.partitionBy(col('environment'),col('code')).orderBy(col('cigcopytime').desc())
    divisions_df = divisions_raw_df.withColumn("row_num", row_number().over(window_spec))

    divisions_df = renameColumns(divisions_df, {
        "code": "divisioncode",
        "blockingstatus": "blockingstatuscode",
        "description": "divisiondescription",
        "customer" : "accountid",
        "businesstype" : "businesstypeid",
        "companysize" : "companysizeid",
        "activitysector" : "sectorid",
        "activitysubsector" : "subsectorid"
    })

    divisions_df = convertDataTypes(
            data_frame = divisions_df,
            integer_cols= ['divisioncode']
        )

    divisions_df = divisions_df.where(col('row_num') == 1)
    divisions_df = divisions_df.drop(
        'account', 'row_num', 'blockingstatus','cigprocessed','code','customer','description','hid',
        'sector','businesstype','companysize','activitysector','activitysubsector','subsector','taxofficenumber',
        'ingestionday', 'ingestionmonth', 'ingestionyear')

    divisions_df.repartition(2).write.mode("overwrite").parquet(s3_destination)

def load_divisiondivisiontypes(database, table_name, s3_destination):
    divisiondivisiontypes_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()
    divisiondivisiontypes_raw_df = cleanDataFrame(divisiondivisiontypes_raw_df, ['id','environment'])
    divisiondivisiontypes_raw_df = convertDataTypes(
        data_frame = divisiondivisiontypes_raw_df,
        timestamp_cols=['cigcopytime', 'syscreated', 'sysmodified'],
        integer_cols= ['divisioncode'],
        boolean_cols = ['cigprocessed'],
        date_cols=['enddate', 'startdate','invoiceduntil']
    )

    window_spec = Window.partitionBy(col('environment'),col('divisioncode'),col('divisiontype')).orderBy(col('cigcopytime').desc(),col('syscreated').desc())
    divisiondivisiontypes_df = divisiondivisiontypes_raw_df.withColumn("row_num", row_number().over(window_spec))
    the_latest_record_index = 1
    divisiondivisiontypes_df = divisiondivisiontypes_df.where(col('row_num') == the_latest_record_index)
    divisiondivisiontypes_df = divisiondivisiontypes_df.drop('row_num', 'ingestionday', 'ingestionmonth', 'ingestionyear')
    divisiondivisiontypes_df.repartition(2).write.mode("overwrite").parquet(s3_destination)


def load_creditmanagementstatus(database, table_name, s3_destination):
    creditmanagementstatus_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()
    creditmanagementstatus_raw_df = cleanDataFrame(creditmanagementstatus_raw_df, ['id','environment'])
    creditmanagementstatus_raw_df = convertDataTypes(
        data_frame = creditmanagementstatus_raw_df,
        timestamp_cols=['cigcopytime', 'syscreated', 'sysmodified'],
        integer_cols= ['division','descriptiontermid','isactive'],
        boolean_cols = ['cigprocessed']
    )

    window_spec = Window.partitionBy(col('id')).orderBy(col('cigcopytime').desc())
    creditmanagementstatus_df = creditmanagementstatus_raw_df.withColumn("row_num", row_number().over(window_spec))
    the_latest_record_index = 1
    creditmanagementstatus_df = creditmanagementstatus_df.where(col('row_num') == the_latest_record_index)
    creditmanagementstatus_df = creditmanagementstatus_df.drop('row_num', 'ingestionday', 'ingestionmonth', 'ingestionyear')

    creditmanagementstatus_df.repartition(2).write.mode("overwrite").parquet(s3_destination)

def load_customersubscriptionstatistics(database, table_name, s3_destination):
    customersubscriptionstatistics_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()
    customersubscriptionstatistics_raw_df = cleanDataFrame(customersubscriptionstatistics_raw_df, ['id','environment'])
    customersubscriptionstatistics_raw_df = convertDataTypes(
    data_frame = customersubscriptionstatistics_raw_df,
    timestamp_cols= ['cigcopytime', 'syscreated'],
    integer_cols= ['type','source'],
    boolean_cols = ['cigprocessed','usesimplewizard']
    )

    window_spec = Window.partitionBy(col('id')).orderBy(col('cigcopytime').desc())
    customersubscriptionstatistics_df = customersubscriptionstatistics_raw_df.withColumn("row_num", row_number().over(window_spec))
    
    customersubscriptionstatistics_df = renameColumns(customersubscriptionstatistics_df, {
        'id':'customersubscriptionstatisticid',
        'account': 'accountid',
        'reseller': 'resellerid',
        'accountant': 'accountantid'
    })

    the_latest_record_index = 1
    customersubscriptionstatistics_df = customersubscriptionstatistics_df.where(col('row_num') == the_latest_record_index)
    customersubscriptionstatistics_df = customersubscriptionstatistics_df.drop('row_num', 'ingestionday', 'ingestionmonth', 'ingestionyear')

    customersubscriptionstatistics_df.repartition(2).write.mode("overwrite").parquet(s3_destination)

def load_divisiontypes(database, table_name, s3_destination):
    divisiontypes_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()
    divisiontypes_raw_df = cleanDataFrame(divisiontypes_raw_df, ['id','environment'])
    divisiontypes_raw_df = convertDataTypes(
        data_frame = divisiontypes_raw_df,
        integer_cols= ['descriptiontermid'],
        long_cols=['modules']
    )

    window_spec = Window.partitionBy(col('environment'),col('code')).orderBy(col('cigcopytime').desc())
    divisiontypes_df = divisiontypes_raw_df.withColumn("row_num", row_number().over(window_spec))
    the_latest_record_index = 1
    divisiontypes_df = divisiontypes_df.where(col('row_num') == the_latest_record_index)
    divisiontypes_df = divisiontypes_df.drop('cigcopytime','cigprocessed','syscreated','sysmodified','syscreator','sysmodifier','row_num', 'ingestionday', 'ingestionmonth', 'ingestionyear')
    divisiontypes_df.repartition(2).write.mode("overwrite").parquet(s3_destination)

def load_itemclasses(database, table_name, s3_destination):
    itemclasses_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()
    itemclasses_raw_df = cleanDataFrame(itemclasses_raw_df, ['id','environment'])
    itemclasses_raw_df = convertDataTypes(
        data_frame = itemclasses_raw_df,
        timestamp_cols=['cigcopytime'],
        integer_cols= ['division','classid','descriptiontermid'],
        boolean_cols = ['cigprocessed']
    )

    window_spec = Window.partitionBy(col('id')).orderBy(col('cigcopytime').desc())
    itemclasses_df = itemclasses_raw_df.withColumn("row_num", row_number().over(window_spec))
    the_latest_record_index = 1
    itemclasses_df = itemclasses_df.where(col('row_num') == the_latest_record_index)
    itemclasses_df = itemclasses_df.drop('row_num', 'ingestionday', 'ingestionmonth', 'ingestionyear')
    itemclasses_df.repartition(2).write.mode("overwrite").parquet(s3_destination)

def load_itemrelations(database, table_name, s3_destination):
    itemrelations_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()
    itemrelations_raw_df = cleanDataFrame(itemrelations_raw_df, ['id','environment'])
    itemrelations_raw_df = convertDataTypes(
        data_frame = itemrelations_raw_df,
        timestamp_cols=['cigcopytime'],
        integer_cols= ['type','division'],
        boolean_cols = ['cigprocessed'],
        double_cols = ['quantity','maxquantity']
    )

    window_spec = Window.partitionBy(col('id')).orderBy(col('cigcopytime').desc())
    itemrelations_df = itemrelations_raw_df.withColumn("row_num", row_number().over(window_spec))
    the_latest_record_index = 1
    itemrelations_df = itemrelations_df.where(col('row_num') == the_latest_record_index)
    itemrelations_df = itemrelations_df.drop('row_num', 'ingestionday', 'ingestionmonth', 'ingestionyear')
    itemrelations_df.repartition(2).write.mode("overwrite").parquet(s3_destination)

def load_leadsources(database, table_name, s3_destination):
    leadsources_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()
    leadsources_raw_df = cleanDataFrame(leadsources_raw_df, ['id','environment'])
    leadsources_raw_df = convertDataTypes(
        data_frame = leadsources_raw_df,
        timestamp_cols=['cigcopytime','sysmodified','cigcopytime','syscreated'],
        integer_cols= ['division','descriptiontermid'],
        boolean_cols = ['cigprocessed','active']
    )

    window_spec = Window.partitionBy(col('id')).orderBy(col('cigcopytime').desc())
    leadsources_df = leadsources_raw_df.withColumn("row_num", row_number().over(window_spec))
    the_latest_record_index = 1
    leadsources_df = leadsources_df.where(col('row_num') == the_latest_record_index)
    leadsources_df = leadsources_df.drop('row_num', 'ingestionday', 'ingestionmonth', 'ingestionyear')
    leadsources_df.repartition(2).write.mode("overwrite").parquet(s3_destination)

def load_opportunities_hosting(database, table_name, s3_destination):
    opportunities_hosting_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()
    opportunities_hosting_raw_df = cleanDataFrame(opportunities_hosting_raw_df, ['id','environment'])
    opportunities_hosting_raw_df = convertDataTypes(
        data_frame = opportunities_hosting_raw_df,
        timestamp_cols=['cigcopytime','closedate','actiondate','syscreated','sysmodified'],
        integer_cols= ['division','number','opportunitytype','opportunitydepartment'],
        boolean_cols = ['cigprocessed','ownerread'],
        double_cols = ['probability','amountfc','amountdc','ratefc']
    )

    window_spec = Window.partitionBy(col('id')).orderBy(col('cigcopytime').desc())
    opportunities_hosting_df = opportunities_hosting_raw_df.withColumn("row_num", row_number().over(window_spec))
    the_latest_record_index = 1
    opportunities_hosting_df = opportunities_hosting_df.where(col('row_num') == the_latest_record_index)
    opportunities_hosting_df = opportunities_hosting_df.drop('row_num', 'reseller', 'accountant', 'involvementtype', 'ingestionday', 'ingestionmonth', 'ingestionyear')
    opportunities_hosting_df.repartition(2).write.mode("overwrite").parquet(s3_destination)

def load_opportunitystages(database, table_name, s3_destination):
    opportunitystages_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()
    opportunitystages_raw_df = cleanDataFrame(opportunitystages_raw_df, ['id','environment'])
   
    opportunitystages_raw_df = convertDataTypes(
    data_frame = opportunitystages_raw_df,
    timestamp_cols=['cigcopytime','syscreated','sysmodified'],
    integer_cols= ['color','descriptiontermid','forecasttype','opportunitystatus','sortorder','division'],
    boolean_cols = ['cigprocessed','active'],
    double_cols = ['probability']
    )

    window_spec = Window.partitionBy(col('id')).orderBy(col('cigcopytime').desc())
    opportunitystages_df = opportunitystages_raw_df.withColumn("row_num", row_number().over(window_spec))
    the_latest_record_index = 1
    opportunitystages_df = opportunitystages_df.where(col('row_num') == the_latest_record_index)
    opportunitystages_df = opportunitystages_df.drop('row_num', 'ingestionday', 'ingestionmonth', 'ingestionyear')

    opportunitystages_df.repartition(2).write.mode("overwrite").parquet(s3_destination)

# Covered End to End
def load_items(database, table_name, s3_destination):
    items_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name = table_name).toDF()
    items_raw_df = cleanDataFrame(items_raw_df, ['id','environment'])

    items_raw_df = convertDataTypes(
        data_frame = items_raw_df,
        long_cols = ['modulecode', 'topictime', 'modulecode', 'topictime'],
        timestamp_cols = [
            'syscreated', 'sysmodified', 'cigcopytime', 
            'freedatefield_01', 'freedatefield_02', 'freedatefield_03', 'freedatefield_04', 'freedatefield_05', 
            'startdate', 'enddate'
        ],
        boolean_cols = [
            'isbatchnumberitem','ismakeitem', 'isnewcontract', 'istime', 'istaxable', 'isondemanditem', 'iswebshopitem',
            'isregistrationcodeitem', 'roundeach', 'roundplannedquantityfactor', 'useexplosion', 'copyremarks',
            'issalesitem', 'isstockitem', 'isserialnumberitem', 'issubassemblyitem', 'isfractionalloweditem',
            'isassetitem', 'ispurchaseitem', 'issubcontracteditem', 'ispackageitem', 'cigprocessed', 'hasbillofmaterial', 
            'freeboolfield_01', 'freeboolfield_02', 'freeboolfield_03', 'freeboolfield_04', 'freeboolfield_05'
        ],
        double_cols = [
            'costpricestandard', 'batchquantity', 'grossweight', 'averagecost', 'netweight', 
            'length', 'width', 'depth', 'margin', 'weightfactor', 'costpricenew', 'calculatorunitfactor', 
            'freenumberfield_01', 'freenumberfield_02', 'freenumberfield_03', 'freenumberfield_04', 
            'freenumberfield_05', 'freenumberfield_06', 'freenumberfield_07', 'freenumberfield_08', 
            'statisticalunits', 'statisticalquantity', 'statisticalvalue', 'statisticalnetweight'
        ],
        integer_cols = [
            'division', 'securitylevel', 'assembledleaddays', 
            'calculatortype', 'calculatorunitfactortype', 'descriptiontermid', 'extradescriptiontermid'
        ] 
    )
    # What to do with picture column? 
    # It is a byte[] automatically converted to varchar
    window_spec = Window.partitionBy(col('id')).orderBy(col('cigcopytime').desc())
    items_df = items_raw_df.withColumn("row_num", row_number().over(window_spec))
    
    items_df = renameColumns(items_df, {
        'id':'itemid',
        'code': 'itemcode',
        'description': 'itemdescription',
        'class_02': 'itemtype'
    })
    the_latest_record_index = 1
    items_df = items_df.where(col('row_num') == the_latest_record_index)
    items_df = items_df.drop('row_num', 'ingestionday', 'ingestionmonth', 'ingestionyear')

    items_df.repartition(1).write.mode("overwrite").parquet(s3_destination)

# Covered End to End
def load_appusagelines(database, table_name, s3_destination):
    appusagelines_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()

    appusagelines_raw_df = convertDataTypes(
        data_frame = appusagelines_raw_df,
        timestamp_cols = ['syscreated', 'sysmodified', 'usedondate', 'cigcopytime'],
        boolean_cols = ['cigprocessed'],
        integer_cols = ['useddivision']
    )
    appusagelines_raw_df = appusagelines_raw_df\
        .withColumn('id', upper(trim(col('id'))))\
        .withColumn('userid', upper(trim(col('userid'))))\
        .withColumn('oauthclient', upper(trim(col('oauthclient'))))\
        .withColumn('syscreator', upper(trim(col('syscreator'))))\
        .withColumn('sysmodifier', upper(trim(col('sysmodifier'))))\
        .withColumn('environment', upper(trim(col('environment'))))
    
    window_spec = Window.partitionBy(col('id')).orderBy(col('cigcopytime').desc())
    appusagelines_df = appusagelines_raw_df.withColumn("row_num", row_number().over(window_spec))
    the_latest_record_index = 1
    appusagelines_df = appusagelines_df.where(col('row_num') == the_latest_record_index)
    appusagelines_df = appusagelines_df.drop('row_num', 'ingestionday', 'ingestionmonth', 'ingestionyear')
    appusagelines_df.repartition(32).write.mode("overwrite").parquet(s3_destination)

# Covered End to End
def load_blockingstatus(database, table_name, s3_destination):
    blockingstatus_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()

    blockingstatus_raw_df = convertDataTypes(
        data_frame = blockingstatus_raw_df,
        timestamp_cols=['cigcopytime'],
        integer_cols = ['descriptionsuffixtermid', 'descriptiontermid']
    )
    blockingstatus_raw_df = blockingstatus_raw_df.withColumn('id', trim(col('id')))
    blockingstatus_raw_df = blockingstatus_raw_df.where(upper(trim(col('environment'))) == 'NL')    
    
    window_spec = Window.partitionBy(col('id')).orderBy(col('cigcopytime').desc()) 
    blockingstatus_df = blockingstatus_raw_df.withColumn("row_num", row_number().over(window_spec))
    blockingstatus_df = renameColumns(blockingstatus_df, {
        'id':'blockingstatuscode',
        'description': 'blockingstatusdescription'
    })
    # Need to convert id after partitioning
    # since AWS makes partitioning column type string by default
    blockingstatus_df = convertDataTypes(
        data_frame = blockingstatus_df,
        integer_cols = ['blockingstatuscode']
    )
    the_latest_record_index = 1
    blockingstatus_df = blockingstatus_df.where(col('row_num') == the_latest_record_index)
    blockingstatus_df = blockingstatus_df.drop('cigprocessed', 'environment', 'row_num', 'ingestionday', 'ingestionmonth', 'ingestionyear')
    blockingstatus_df.repartition(1).write.mode("overwrite").parquet(s3_destination)

# Covered End to End
def load_businesstypes(database, table_name, s3_destination):
    businesstypes_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()

    businesstypes_raw_df = convertDataTypes(
        data_frame = businesstypes_raw_df,
        timestamp_cols=['syscreated', 'sysmodified', 'cigcopytime'],
        integer_cols = ['descriptiontermid', 'shortdescriptiontermid'],
        boolean_cols = ['cigprocessed']
    )
    businesstypes_raw_df = businesstypes_raw_df.withColumn('environment', upper(trim(col('environment'))))
    businesstypes_raw_df = businesstypes_raw_df.where(col('environment') == 'NL')
    window_spec = Window.partitionBy(col('id')).orderBy(col('cigcopytime').desc())
    businesstypes_df = businesstypes_raw_df.withColumn("row_num", row_number().over(window_spec)) 
    the_latest_record_index = 1
    businesstypes_df = businesstypes_df.where(col('row_num') == the_latest_record_index)
    businesstypes_df = businesstypes_df.drop('row_num', 'ingestionday', 'ingestionmonth', 'ingestionyear')

    businesstypes_df.repartition(1).write.mode("overwrite").parquet(s3_destination)

# Covered End to End
def load_contractmutations(database, table_name, s3_destination):
    contractmutations_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()

    contractmutations_raw_df = convertDataTypes(
        data_frame = contractmutations_raw_df,
        timestamp_cols=['syscreated', 'sysmodified', 'eventdate', 'cigcopytime'],
        integer_cols = ['division', 'quantity'],
        long_cols = ['hid'],
        boolean_cols = ['cigprocessed'],
        double_cols = ['contractlinevalue']
    )
    contractmutations_raw_df = contractmutations_raw_df.withColumn('environment', upper(trim(col('environment'))))
    window_spec = Window.partitionBy('id').orderBy(col('cigcopytime').desc())
    contractmutations_df = contractmutations_raw_df.withColumn("row_num", row_number().over(window_spec))
    the_latest_record_index = 1
    contractmutations_df = contractmutations_df.where(col('row_num') == the_latest_record_index)
    contractmutations_df = contractmutations_df.drop('row_num','ingestionday', 'ingestionmonth', 'ingestionyear')
    
    contractmutations_df.repartition(5).write.mode("overwrite").parquet(s3_destination)

# Covered End to End
def load_contractevents(database, table_name, s3_destination):
    contractevents_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()
    
    contractevents_raw_df = convertDataTypes(
        data_frame = contractevents_raw_df,
        timestamp_cols=['syscreated', 'sysmodified', 'cigcopytime'],
        integer_cols = ['descriptiontermid', 'creationtype'],
        boolean_cols = ['cigprocessed']
    )
    contractevents_raw_df = contractevents_raw_df.withColumn('environment', upper(trim(col('environment'))))

    window_spec = Window.partitionBy('id').orderBy(col('cigcopytime').desc())    
    the_latest_record_index = 1
    contractevents_df = contractevents_raw_df\
        .withColumn("row_num", row_number().over(window_spec))\
        .where(col('row_num') == the_latest_record_index)\
        .drop('row_num', 'ingestionday', 'ingestionmonth', 'ingestionyear')
    contractevents_df.repartition(1).write.mode("overwrite").parquet(s3_destination)

# Covered End to End
def load_accountsclassifications(database, table_name, s3_destination):
    accountsclassifications_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()

    accountsclassifications_raw_df = convertDataTypes(
        data_frame = accountsclassifications_raw_df,
        timestamp_cols = ['syscreated', 'sysmodified', 'cigcopytime'],
        boolean_cols = ['cigprocessed'],
        integer_cols = ['division', 'descriptiontermid']
    )
    accountsclassifications_raw_df = accountsclassifications_raw_df\
        .withColumn('id', upper(trim(col('id'))))\
        .withColumn('creditmanagementscenario', upper(trim(col('creditmanagementscenario'))))\
        .withColumn('syscreator', upper(trim(col('syscreator'))))\
        .withColumn('sysmodifier', upper(trim(col('sysmodifier'))))\
        .withColumn('environment', upper(trim(col('environment'))))

    window_spec = Window.partitionBy(col('id')).orderBy(col('cigcopytime').desc())
    accountsclassifications_df = accountsclassifications_raw_df\
        .withColumn("row_num", row_number().over(window_spec))
    
    accountsclassifications_df = renameColumns(accountsclassifications_df, {
        'code': 'accountclassificationcode',
        'description': 'accountclassificationdescription'
    })
    the_latest_record_index = 1
    accountsclassifications_df = accountsclassifications_df.where(col('row_num') == the_latest_record_index)
    accountsclassifications_df = accountsclassifications_df.drop(        'row_num', 'ingestionday', 'ingestionmonth', 'ingestionyear')
    accountsclassifications_df.repartition(1).write.mode("overwrite").parquet(s3_destination)

# Covered End to End
def load_contractlines(database, table_name, s3_destination):
    contractlines_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()

    contractlines_raw_df = convertDataTypes(
        data_frame = contractlines_raw_df,
        timestamp_cols = ['syscreated', 'sysmodified', 'cigcopytime', 'startdate', 'finaldate', 'cancellationdate'],
        boolean_cols = ['cigprocessed'],
        double_cols = ['price', 'quantity']
    )
    contractlines_raw_df = contractlines_raw_df\
        .withColumn('id', upper(trim(col('id'))))\
        .withColumn('contractid', upper(trim(col('contractid'))))\
        .withColumn('syscreator', upper(trim(col('syscreator'))))\
        .withColumn('sysmodifier', upper(trim(col('sysmodifier'))))\
        .withColumn('environment', upper(trim(col('environment'))))

    window_spec = Window.partitionBy(col('id')).orderBy(col('cigcopytime').desc())
    contractlines_df = contractlines_raw_df\
        .withColumn("row_num", row_number().over(window_spec))
    the_latest_record_index = 1
    contractlines_df = contractlines_df.where(col('row_num') == the_latest_record_index)
    contractlines_df = contractlines_df.drop(
        'row_num', 'ingestionday', 'ingestionmonth', 'ingestionyear',
        'cancellationeventid', 'contracteventid', 'discount', 'enddate',
        'item','itemnumber','itemprice','linedescription',
        'prolongdaypart', 'unitfactor', 'usagequantity')

    contractlines_df.repartition(1).write.mode("overwrite").parquet(s3_destination)

def load_saleshandoverdocument(database, table_name, s3_destination):
    documents_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()

    documents_raw_df = cleanDataFrame(documents_raw_df, ['id'])
    documents_raw_df = convertDataTypes(
            data_frame = documents_raw_df,
            integer_cols = ['type', 'hid', 'ownertype', 'status', 'source'],
            timestamp_cols = ['cigcopytime', 'sysmodified', 'startdate', 'enddate', 'syscreated']
        )
    documents_raw_df = documents_raw_df.where(col('type') == 193)

    window_spec = Window.partitionBy(col('id')).orderBy(col('cigcopytime').desc(), col('sysmodified').desc())
    saleshandoverdocument_df = documents_raw_df.withColumn("row_num", row_number().over(window_spec))
    saleshandoverdocument_df = saleshandoverdocument_df.where(col('row_num') == 1)

    saleshandoverdocument_df = renameColumns(saleshandoverdocument_df, {
        'id':'documentid',
        'account': 'accountid',
        'contact': 'contactid',
        'item': 'itemid',
        'owner': 'ownerid'
    })

    saleshandoverdocument_df = saleshandoverdocument_df.drop(
            'row_num', 'ingestionday', 'ingestionmonth', 'ingestionyear', 
            'parent', 'category', 'viewdate', 'type', 'division', 'newstype', 'entryid', 'ownertypevalue', 'votecount', 'votesum', 
            'itemnumber', 'fpintrotext', 'paymentreference', 'yourref', 'quotationnumber', 'contractnumber', 'currency', 'documentdate', 
            'layout', 'templatetype', 'ddview', 'project', 'itemgroup', 'share', 'employee', 'opportunity', 'lead', 'emailimportance', 
            'emailsent', 'subscription', 'stockentrynumber', 'pages', 'ordernumber', 'campaign', 'sourcemailmessageid', 'asset', 'shopordernumber', 
            'documentfolder', 'extension', 'itemroutingstep', 'itemmaterial', 'cigcopytime', 'environment', 'cigprocessed'
    )

    saleshandoverdocument_df.repartition(1).write.mode("overwrite").parquet(s3_destination)

def load_subscriptionquotations(database, table_name, s3_destination):
    subscriptionquotations_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()

    subscriptionquotations_raw_df = cleanDataFrame(subscriptionquotations_raw_df, ['id'])

    subscriptionquotations_raw_df = convertDataTypes(
            data_frame = subscriptionquotations_raw_df,
            integer_cols = ['mainline', 'discountperiod', 'status', 'discounttype', 'firstyearnocancellation'],
            double_cols = ['quantity', 'discountamount', 'totalmrr', 'extraservicenumberofdays', 'extraservicetotaldiscountamount', 'extraservicegrossamount'],
            timestamp_cols = ['cigcopytime', 'syscreated', 'sysmodified']
        )

    window_spec = Window.partitionBy(col('id')).orderBy(col('cigcopytime').desc())
    subscriptionquotations_df = subscriptionquotations_raw_df.withColumn("row_num", row_number().over(window_spec))
    subscriptionquotations_df = subscriptionquotations_df.where(col('row_num') == 1)

    subscriptionquotations_df = renameColumns(subscriptionquotations_df, {
        'id':'subscriptionquotationid',
        'customer': 'accountid',
        'item': 'itemid'
    })

    subscriptionquotations_df = subscriptionquotations_df.drop('row_num', 'ingestionday', 'ingestionmonth', 'ingestionyear', 'cigprocessed')

    subscriptionquotations_df.repartition(1).write.mode("overwrite").parquet(s3_destination)

def load_surveyresults(database, table_name, s3_destination):
    surveyresults_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()

    surveyresults_raw_df = cleanDataFrame(surveyresults_raw_df, ['id'])

    surveyresults_raw_df = convertDataTypes(
            data_frame = surveyresults_raw_df,
            boolean_cols = ['cigprocessed'],
            integer_cols = ['questioncode', 'objectiveanswer', 'fordivision'],
            timestamp_cols = ['createddate', 'cigcopytime']
        )

    window_spec = Window.partitionBy(col('id')).orderBy(col('cigcopytime').desc())
    surveyresults_df = surveyresults_raw_df.withColumn("row_num", row_number().over(window_spec))
    surveyresults_df = surveyresults_df.where(col('row_num') == 1)

    surveyresults_df = renameColumns(surveyresults_df, {
        'id':'surveyresultid',
        'customer': 'accountid',
        'fordivision': 'divisioncode'
    })

    surveyresults_df = surveyresults_df.drop('row_num', 'ingestionday', 'ingestionmonth', 'ingestionyear')

    surveyresults_df.repartition(1).write.mode("overwrite").parquet(s3_destination)

    return surveyresults_df

def load_surveyresults_notlinktoaccountant(surveyresults_df, s3_destination):
    surveyresults_df = cleanDataFrame(surveyresults_df, ['accountid'])

    window_spec = Window.partitionBy(col('accountid')).orderBy(col('createddate').desc())
    surveyresults_df = surveyresults_df.withColumn("row_num", row_number().over(window_spec))
    surveyresults_df = surveyresults_df.where((col('row_num') == 1) & (col("questioncode") == 0))

    surveyresults_df = convertDataTypes(
            data_frame = surveyresults_df,
            integer_cols = ['divisioncode', 'objectiveanswer'],
            timestamp_cols = ['createddate']
        )

    surveyresults_notlinktoaccountant_df = renameColumns(surveyresults_df, {
        'createddate': 'submitteddate'
    })

    surveyresults_notlinktoaccountant_df = surveyresults_notlinktoaccountant_df.withColumn("objectiveanswerdescription", 
        when(col('objectiveanswer') == 0, 'My accountant does not work with EOL')\
        .when(col('objectiveanswer') == 1, 'Have not chosen accountant')\
        .when(col('objectiveanswer') == 2, 'No need external accountant')\
        .when(col('objectiveanswer') == 3, 'Disallow accountant direct access')\
        .when(col('objectiveanswer') == 4, 'Other reasons')\
        .when(col('objectiveanswer') == 5, 'Intend to link accountant but skip the step later (* This answer is not visible to the user)')
    )

    surveyresults_notlinktoaccountant_df = surveyresults_notlinktoaccountant_df.drop('surveyresultid', 'questioncode', 'cigcopytime', 'environment', 'cigprocessed', 'row_num')

    surveyresults_notlinktoaccountant_df.repartition(1).write.mode("overwrite").parquet(s3_destination)

def load_units(database, table_name, s3_destination):
    units_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()
    units_raw_df = cleanDataFrame(units_raw_df, ['id'])

    units_raw_df = convertDataTypes(
            data_frame = units_raw_df,
            boolean_cols = ['active', 'cigprocessed'],
            integer_cols = ['division', 'main', 'descriptiontermid'],
            timestamp_cols = ['cigcopytime']
        )

    window_spec = Window.partitionBy(col('id')).orderBy(col('cigcopytime').desc())
    units_df = units_raw_df.withColumn("row_num", row_number().over(window_spec))
    units_df = units_df.where(col('row_num') == 1)

    units_df = units_df.drop('row_num', 'ingestionday', 'ingestionmonth', 'ingestionyear')

    units_df.repartition(1).write.mode("overwrite").parquet(s3_destination)

def load_usagetransactions(database, table_name, s3_destination):
    usagetransactions_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()
    usagetransactions_raw_df = cleanDataFrame(usagetransactions_raw_df, ['id'])

    usagetransactions_raw_df = convertDataTypes(
            data_frame = usagetransactions_raw_df,
            boolean_cols =  ['cigprocessed'],
            integer_cols = ['division', 'quantity', 'sourcedivision'],
            double_cols = ['amount', 'costprice', 'salesprice'],
            timestamp_cols = ['cigcopytime', 'startdate', 'enddate', 'sysmodified', 'syscreated']
        )


    window_spec = Window.partitionBy(col('id')).orderBy(col('cigcopytime').desc())
    usagetransactions_df = usagetransactions_raw_df.withColumn("row_num", row_number().over(window_spec))
    usagetransactions_df = usagetransactions_df.where((col('row_num') == 1) & (col('cigcopytime') >= datetime(year=2018, month=10, day=5)))

    usagetransactions_df = usagetransactions_df.drop('row_num', 'ingestionday', 'ingestionmonth', 'ingestionyear')

    usagetransactions_df.repartition(10).write.mode("overwrite").parquet(s3_destination)

def load_usertypes(database, table_name, s3_destination):
    usertypes_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()
    usertypes_raw_df = cleanDataFrame(usertypes_raw_df, ['id', 'item', 'environment'])

    usertypes_raw_df = convertDataTypes(
            data_frame = usertypes_raw_df,
            integer_cols = ['descriptiontermid', 'startpage', 'gettingstartedpage'], 
            timestamp_cols = ['cigcopytime', 'syscreated', 'sysmodified']
        )

    window_spec = Window.partitionBy(col('id'), col('item')).orderBy(col('cigcopytime').desc(), col('environment').desc())
    usertypes_df = usertypes_raw_df.withColumn("row_num", row_number().over(window_spec))
    usertypes_df = usertypes_df.where(col('row_num') == 1)

    usertypes_df = renameColumns(usertypes_df, {
        'id': 'usertypeid',
        'code' : 'usertypecode',
        'description': 'usertypedescription',
        'descriptiontermid': 'usertypedescriptiontermid'
    })

    usertypes_df = usertypes_df.drop('row_num', 'ingestionday', 'ingestionmonth', 'ingestionyear', 'cigprocessed')

    usertypes_df.repartition(1).write.mode("overwrite").parquet(s3_destination)

def load_userusertypes(database, table_name, s3_destination):
    userusertypes_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()
    userusertypes_raw_df = cleanDataFrame(userusertypes_raw_df, ['userid'])

    userusertypes_raw_df = convertDataTypes(
        data_frame = userusertypes_raw_df,
        date_cols = ['invoiceduntil'],
        timestamp_cols = ['cigcopytime', 'startdate', 'enddate', 'syscreated', 'sysmodified']
    )

    window_spec = Window.partitionBy(col('userid')).orderBy(col('cigcopytime').desc(), col('syscreated').desc())
    userusertypes_df = userusertypes_raw_df.withColumn("row_num", row_number().over(window_spec))
    userusertypes_df = userusertypes_df.where(col('row_num') == 1)

    userusertypes_df = renameColumns(userusertypes_df, {
        'usertype': 'usertypeid'
    })

    userusertypes_df = userusertypes_df.drop('row_num', 'ingestionday', 'ingestionmonth', 'ingestionyear', 'id', 'cigprocessed')

    userusertypes_df.repartition(1).write.mode("overwrite").parquet(s3_destination)

def load_projects(database, table_name, s3_destination):
    projects_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()
    projects_raw_df = cleanDataFrame(projects_raw_df, ['id'])

    projects_raw_df = convertDataTypes(
            data_frame = projects_raw_df,
            timestamp_cols = ['cigcopytime', 'startdate', 'enddate', 'syscreated', 'sysmodified'],
            double_cols = ['salestimequantity', 'salesamountdc', 'timequantitytoalert', 'costsamountfc', 'purchasemarkuppercentage', 'budgetsalesamountdc', 'budgetcostsamountdc'],
            integer_cols = ['descriptiontermid', 'type', 'prepaidtype', 'usebillingmilestones', 'allowadditionalinvoicing', 'blockentry', 'blockrebilling', 'budgettype', 'invoiceasquoted', 'financialyear'] 
        )

    window_spec = Window.partitionBy(col('id')).orderBy(col('cigcopytime').desc())
    projects_df = projects_raw_df.withColumn("row_num", row_number().over(window_spec))
    projects_df = projects_df.where(col('row_num') == 1)

    projects_df = renameColumns(projects_df, {
        'id':'projectid',
        'code': 'projectcode',
        'account': 'accountid',
        'accountcontact': 'accountcontactid',
        'manager': 'managerid',
        'classification': 'classificationid'
    })

    projects_df = projects_df.drop('row_num', 'ingestionday', 'ingestionmonth', 'ingestionyear', 'division', 'cigprocessed')
    projects_df.repartition(1).write.mode("overwrite").parquet(s3_destination)

def load_timecosttransactions(database, table_name, s3_destination):
    timecosttransactions_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()
    timecosttransactions_raw_df = cleanDataFrame(timecosttransactions_raw_df, ['id'])

    timecosttransactions_raw_df = convertDataTypes(
            data_frame = timecosttransactions_raw_df,
            timestamp_cols = ['cigcopytime', 'date', 'starttime', 'endtime', 'syscreated', 'sysmodified'],
            integer_cols = ['type', 'hourstatus', 'entrynumber', 'source', 'manufacturingtimetype', 'setupcomplete', 'backflushed', 'isoperationfinished'],
            double_cols = ['quantity', 'pricefc', 'amountfc', 'laborhours', 'producedquantity', 'percentcomplete', 'laborrate', 'laborburdenrate', 'machineburdenrate', 'generalburdenrate', 'amountdc', 'ratefc', 'purchaseamountfc', 'purchasemarkuppercentage', 'internalratedc', 'costamountdc']
        )


    window_spec = Window.partitionBy(col('id')).orderBy(col('cigcopytime').desc())
    timecosttransactions_df = timecosttransactions_raw_df.withColumn("row_num", row_number().over(window_spec))
    timecosttransactions_df = timecosttransactions_df.where(col('row_num') == 1)

    timecosttransactions_df = renameColumns(timecosttransactions_df, {
        'id':'timecosttransactionid',
        'project': 'projectid',
        'account': 'accountid',
        'employee': 'employeeid',
        'item': 'itemid'
    })

    timecosttransactions_df = timecosttransactions_df.drop('row_num', 'ingestionday', 'ingestionmonth', 'ingestionyear', 'division', 'cigprocessed')
    timecosttransactions_df.repartition(1).write.mode("overwrite").parquet(s3_destination)

def load_usageentitlements(database, table_name, s3_destination):
    usageentitlements_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()
    usageentitlements_raw_df = cleanDataFrame(usageentitlements_raw_df, ['id'])

    usageentitlements_raw_df = convertDataTypes(
            data_frame = usageentitlements_raw_df,
            timestamp_cols = ['cigcopytime', 'startdate', 'enddate', 'expirationdate', 'syscreated', 'sysmodified'],
            double_cols = ['unitprice'],
            integer_cols = ['originalquantity', 'actualquantity']
        )


    window_spec = Window.partitionBy(col('id')).orderBy(col('cigcopytime').desc())
    usageentitlements_df = usageentitlements_raw_df.withColumn("row_num", row_number().over(window_spec))
    usageentitlements_df = usageentitlements_df.where(col('row_num') == 1)

    usageentitlements_df = renameColumns(usageentitlements_df, {
        'id':'usageentitlementid',
        'account': 'accountid',
        'item': 'itemid'
    })

    usageentitlements_df = usageentitlements_df.drop('row_num', 'ingestionday', 'ingestionmonth', 'ingestionyear', 'division', 'cigprocessed')
    usageentitlements_df.repartition(1).write.mode("overwrite").parquet(s3_destination)

def get_conversationstatus_description(code):
    conversationsource_code_description_map = {
        3 : "csChecking",
        5 : "csUploading",
        8 : "csUploadFailed",
        10 : "csUploaded",
        11 : "csInsertOnHold",
        15 : "csInserting",
        18 : "csInsertFailed",
        20 : "csInserted",
        21 : "csConvertOnHold",
        25 : "csConverting",
        28 : "csConvertFailed",
        30 : "csReady",
        35 : "csWaitingToBeMoved",
        38 : "csHandoverFailed",
        40 : "csHandedOver"
    }
    if not isinstance(code, int):
        return None
    if code in conversationsource_code_description_map:
        return conversationsource_code_description_map[code]
    return None

def load_conversionsource(database, conversionstatus_table_name, gltransactionsources_table_name, s3_destination):
    conversionstatus_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=conversionstatus_table_name).toDF()
    gltransactionsources_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=gltransactionsources_table_name).toDF()
    gltransactionsources_raw_df = cleanDataFrame(gltransactionsources_raw_df, ['id', 'environment'])
    conversionstatus_raw_df = cleanDataFrame(conversionstatus_raw_df, ['environment'])

    the_latest_record_index = 1

    conversionstatus_raw_df = convertDataTypes(
        data_frame = conversionstatus_raw_df,
        timestamp_cols=['cigcopytime', 'syscreated', 'sysmodified'],
        integer_cols = ['division', 'conversionsource', 'hid', 'cigprocessed', 'status']
    )

    window_spec = Window.partitionBy(col('environment'), col('division')).orderBy(col('cigcopytime').desc()) 
    conversionstatus_raw_df = conversionstatus_raw_df.withColumn("row_num", row_number().over(window_spec))
    conversionstatus_df = conversionstatus_raw_df.where(col('row_num') == the_latest_record_index)

    gltransactionsources_raw_df = convertDataTypes(
        data_frame = gltransactionsources_raw_df,
        timestamp_cols=['cigcopytime']
    )

    conversionstatus_df = convertDataTypes(
        data_frame = conversionstatus_df,
        integer_cols=['division']
    )

    window_spec = Window.partitionBy(col('id'), col('environment')).orderBy(col('cigcopytime').desc()) 
    gltransactionsources_df = gltransactionsources_raw_df.withColumn("row_num", row_number().over(window_spec))

    gltransactionsources_df = convertDataTypes(
        data_frame = gltransactionsources_df,
        integer_cols=['id']
    )

    gltransactionsources_df = gltransactionsources_df.where((col('environment') == 'NL') & (col("row_num") == the_latest_record_index))

    get_conversationstatus_description_udf = udf(
        lambda code: get_conversationstatus_description(code),
        StringType()
    )

    conversionsource_df = conversionstatus_df.alias("cs")\
            .join(gltransactionsources_df.alias("glts"), col("cs.conversionsource") == col("glts.id"), "inner")\
            .withColumn("conversionstatusdescription", get_conversationstatus_description_udf(col("cs.status")))\
            .select("cs.environment", col("cs.division").alias("divisioncode"), "cs.idkey", col("cs.status").alias("conversionstatuscode"),
                    "conversionstatusdescription", "cs.userid", col("cs.document").alias("documentid"), "cs.conversionsource", 
                    col("glts.descriptionsuffix").alias("conversionsourcedescription"), "cs.dirpath", "cs.syscreated", "cs.sysmodified")
    
    conversionsource_df.repartition(1).write.mode("overwrite").parquet(s3_destination)

def load_mail2eol(database, table_name, s3_destination):
    mail2eol_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()
    mail2eol_df = convertDataTypes(
        data_frame = mail2eol_raw_df,
        timestamp_cols=['logdatetime'],
        integer_cols = ['activitytype', 'messagetype']
    )
    mail2eol_df.repartition(1).write.mode("overwrite").parquet(s3_destination)

def load_scanprovideruploadlog(database, table_name, s3_destination):
    scanprovideruploadlog_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()
    scanprovideruploadlog_df = convertDataTypes(
        data_frame = scanprovideruploadlog_raw_df,
        timestamp_cols=['logdatetime'],
        integer_cols = ['activitytype', 'fordivision']
    )
    scanprovideruploadlog_df.repartition(1).write.mode("overwrite").parquet(s3_destination)

def load_scanninglyanthe(database, table_name, s3_destination):
    scanninglyanthe_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()
    scanninglyanthe_df = convertDataTypes(
        data_frame = scanninglyanthe_raw_df,
        timestamp_cols=['logdatetime'],
        integer_cols = ['pickedupduration', 'returnedduration', 'totalduration', 'messagestatusinternal']
    )
    scanninglyanthe_df.repartition(1).write.mode("overwrite").parquet(s3_destination)

def load_modules(database, contracts_table_name, accountscontract_summary_table_name, accounts_table_name, packageclassification_table_name, s3_destination):
    contracts_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=contracts_table_name).toDF()
    accountscontract_summary_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name = accountscontract_summary_table_name).toDF()
    accounts_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name = accounts_table_name).toDF()
    packageclassification_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name = packageclassification_table_name).toDF()

    window_spec = Window.partitionBy(col('environment'), col("accountcode"), col("itemcode")).orderBy(col('eventdate').desc(), col("inflowoutflow"))
    modules_df = contracts_df.withColumn("row_num", row_number().over(window_spec))\
                        .where((col('itemtype') == 'Module') & (col('inflowoutflow') == 'Inflow') & (col('row_num') == 1))\
                        .alias("c")\
                        .join(accountscontract_summary_df.alias("acs"), 
                                (col("c.environment") == col("acs.environment")) & 
                                (col("c.accountcode") == col("acs.accountcode")),
                            "inner")\
                        .join(accounts_df.alias("ac"), 
                                (col("c.environment") == col("ac.environment")) & 
                                (col("c.accountcode") == col("ac.accountcode")),
                            "inner")\
                        .join(packageclassification_df.alias("pc"), 
                                (col("acs.environment") == col("pc.environment")) & 
                                (col("acs.latestcommpackage") == col("pc.packagecode")),
                            "left")\
                        .where((col('acs.churned') == 0) & (col('ac.accountclassificationcode').isin('AC7', 'AC1', 'JB0', 'EOL', 'ACC')))\
                        .select(
                            'ac.accountid', 'c.environment', 'c.accountcode', 'c.eventyearmonth', 'c.eventyear', 'c.eventmonth', 'c.eventdate', 'c.eventtype',
                            'c.eventdescription', 'c.inflowoutflow', 'c.itemcode', 'c.itemdescription', 'c.valuepermonth'
                        )

    modules_df.repartition(1).write.mode("overwrite").parquet(s3_destination)

def load_divisionstartuptype(s3_destination):
    global spark
    columns = ['startuptype', 'startuptypedescription']
    vals = [
        ('E', 'Empty'), 
        ('X', 'LegislationTemplate'),
        ('Y', 'LegislationDemo'),
        ('A', 'DivisionTemplate'),
        ('D', 'DivisionCopy'),
        ('R', 'DivisionReturn'),
        ('C', 'Conversion'),
        ('U', 'Unknown')
    ]

    df = spark.createDataFrame(vals, columns)

    df.repartition(1).write.mode("overwrite").parquet(s3_destination)

def load_active_user_divisions(database, table_name, s3_destination):
    active_user_divisions_raw_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()
    active_user_divisions_raw_df = cleanDataFrame(active_user_divisions_raw_df, ['environment','id','userid'])
    active_user_divisions_raw_df = convertDataTypes(
        data_frame = active_user_divisions_raw_df,
        timestamp_cols=['cigcopytime'],
        integer_cols= ['division'],
    )
    active_user_divisions_raw_df = active_user_divisions_raw_df.withColumn('environment', upper(trim(col('environment'))))
    window_spec = Window.partitionBy(col('environment'),col('id')).orderBy(col('cigcopytime').desc())
    active_user_divisions_raw_df = active_user_divisions_raw_df.withColumn("row_num", row_number().over(window_spec))

    active_user_divisions_raw_df = active_user_divisions_raw_df.where(col('row_num') == 1)
    active_user_divisions_raw_df = active_user_divisions_raw_df.drop('cigprocessed','ingestionday', 'ingestionmonth',
                                                                     'ingestionyear','row_num')

    active_user_divisions_raw_df.repartition(2).write.mode("overwrite").parquet(s3_destination)

def load_active_user_roles(database, table_name, s3_destination):
    load_active_user_roles_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()
    load_active_user_roles_df = cleanDataFrame(load_active_user_roles_df, ['environment','id','userid','userroleid'])
    load_active_user_roles_df = convertDataTypes(
        data_frame = load_active_user_roles_df,
        timestamp_cols=['cigcopytime'],
        integer_cols= ['role','rolelevel'],
    )
    load_active_user_roles_df = load_active_user_roles_df.withColumn('environment', upper(trim(col('environment'))))
    window_spec = Window.partitionBy(col('environment'),col('id')).orderBy(col('cigcopytime').desc())
    load_active_user_roles_df = load_active_user_roles_df.withColumn("row_num", row_number().over(window_spec))

    load_active_user_roles_df = load_active_user_roles_df.where(col('row_num') == 1)
    load_active_user_roles_df = load_active_user_roles_df.drop('cigprocessed','ingestionday', 'ingestionmonth','row_num',
                                                                     'ingestionyear')

    load_active_user_roles_df.repartition(8).write.mode("overwrite").parquet(s3_destination)

def load_divisiontypefeaturesets(database, table_name, s3_destination):
    divisiontypefeaturesets_df = glueContext.create_dynamic_frame.from_catalog(database = database, table_name=table_name).toDF()
    
    divisiontypefeaturesets_df = convertDataTypes(
        data_frame = divisiontypefeaturesets_df,
        timestamp_cols=['syscreated', 'sysmodified', 'cigcopytime'],
        integer_cols = ['featureset'],
        boolean_cols = ['cigprocessed']
    )
    divisiontypefeaturesets_df = cleanDataFrame(divisiontypefeaturesets_df, ['id','environment', 'sysmodifier', 'syscreator'])
    window_spec = Window.partitionBy(col('environment'),col('id')).orderBy(col('cigcopytime').desc(), col('syscreated').desc())
    divisiontypefeaturesets_df = divisiontypefeaturesets_df.withColumn("row_num", row_number().over(window_spec))
    divisiontypefeaturesets_df = divisiontypefeaturesets_df.where(col('row_num') == 1)
    divisiontypefeaturesets_df.drop('row_num')

    divisiontypefeaturesets_df.repartition(1).write.mode("overwrite").parquet(s3_destination)

if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, [
        'raw_db', 
        'domain_db', 
        'raw_eol_activitysectors_table', 's3_eol_activitysectors_destination',
        'raw_eol_companysizes_table', 's3_eol_companysizes_destination',
        'raw_eol_contacts_table', 's3_eol_contacts_destination',
        'raw_eol_divisions_table', 's3_eol_divisions_destination',
        'raw_eol_items_table', 's3_eol_items_destination',
        'raw_eol_users_table', 's3_eol_users_destination',
        'raw_eol_appusagelines_table', 's3_eol_appusagelines_destination',
        'raw_eol_blockingstatus_table', 's3_eol_blockingstatus_destination',
        'raw_eol_businesstypes_table', 's3_eol_businesstypes_destination',
        'raw_eol_contractmutations_table', 's3_eol_contractmutations_destination',
        'raw_eol_contractevents_table', 's3_eol_contractevents_destination',
        'raw_eol_accountsclassifications_table', 's3_eol_accountsclassifications_destination',
        'raw_eol_contractlines_table', 's3_eol_contractlines_destination',
        'raw_eol_persons_table', 's3_eol_persons_destination',
        'raw_eol_divisiondivisiontypes_table', 's3_eol_divisiondivisiontypes_destination',
        'raw_eol_customersubscriptionstatistics_table', 's3_eol_customersubscriptionstatistics_destination',
        'raw_eol_creditmanagementstatus_table', 's3_eol_creditmanagementstatus_destination',
        'raw_eol_divisiontypes_table', 's3_eol_divisiontypes_destination',
        'raw_eol_itemclasses_table', 's3_eol_itemclasses_destination',
        'raw_eol_itemrelations_table', 's3_eol_itemrelations_destination',
        'raw_eol_leadsources_table', 's3_eol_leadsources_destination',
        'raw_eol_opportunities_hosting_table','s3_eol_opportunities_hosting_destination',
        'raw_eol_opportunitystages_table','s3_eol_opportunitystages_destination',
        'raw_eol_documents_table', 's3_eol_saleshandoverdocument_destination',
        'raw_eol_subscriptionquotations_table', 's3_eol_subscriptionquotations_destination',
        'raw_eol_surveyresults_table', 's3_eol_surveyresults_destination',
        's3_eol_surveyresults_notlinktoaccountant_destination',
        'raw_eol_units_table', 's3_eol_units_destination',
        'raw_eol_usagetransactions_table', 's3_eol_usagetransactions_destination',
        'raw_eol_usertypes_table', 's3_eol_usertypes_destination',
        'raw_eol_userusertypes_table', 's3_eol_userusertypes_destination',
        'raw_eol_projects_table', 's3_eol_projects_destination',
        'raw_eol_timecosttransactions_table', 's3_eol_timecosttransactions_destination',
        'raw_eol_usageentitlements_table', 's3_eol_usageentitlements_destination',
        'raw_eol_conversionstatus_table', 'raw_eol_gltransactionsources_table', 's3_eol_conversionstatus_destination',
        'raw_eol_mail2eol_table', 's3_eol_mail2eol_destination',
        'raw_eol_scanprovideruploadlog_table', 's3_eol_scanprovideruploadlog_destination',
        'raw_eol_scanninglyanthe_table', 's3_eol_scanninglyanthe_destination','raw_eol_oauthclients_table',
        's3_eol_oauthclientslog_view_destination','s3_eol_oauthclients_view_destination',
        'domain_contracts_table', 'domain_accountscontract_summary_table', 'domain_accounts_table',
        'domain_packageclassification_table', 's3_eol_modules_destination', 's3_eol_divisionstartuptype_destination',
        'raw_eol_activeuserdivisions_table','s3_eol_activeuserdivisions_destination',
        'raw_eol_activeuserroles_table', 's3_eol_activeuserroles_destination',
        'raw_eol_divisiontypefeaturesets_table', 's3_eol_divisiontypefeaturesets_destination'
    ])

    raw_db = args['raw_db']
    domain_db = args['domain_db']
    load_activitysectors(raw_db, args['raw_eol_activitysectors_table'], args['s3_eol_activitysectors_destination'])
    load_companysizes(raw_db, args['raw_eol_companysizes_table'], args['s3_eol_companysizes_destination'])
    load_contacts(raw_db, args['raw_eol_contacts_table'], args['s3_eol_contacts_destination'])
    load_divisions(raw_db, args['raw_eol_divisions_table'], args['s3_eol_divisions_destination'])
    load_items(raw_db, args['raw_eol_items_table'], args['s3_eol_items_destination'])
    load_users(raw_db, args['raw_eol_users_table'], args['s3_eol_users_destination'])
    load_appusagelines(raw_db, args['raw_eol_appusagelines_table'], args['s3_eol_appusagelines_destination'])
    load_blockingstatus(raw_db, args['raw_eol_blockingstatus_table'], args['s3_eol_blockingstatus_destination'])
    load_businesstypes(raw_db, args['raw_eol_businesstypes_table'], args['s3_eol_businesstypes_destination'])
    load_contractmutations(raw_db, args['raw_eol_contractmutations_table'], args['s3_eol_contractmutations_destination'])
    load_contractevents(raw_db, args['raw_eol_contractevents_table'], args['s3_eol_contractevents_destination'])
    load_accountsclassifications(raw_db, args['raw_eol_accountsclassifications_table'], args['s3_eol_accountsclassifications_destination'])
    load_contractlines(raw_db, args['raw_eol_contractlines_table'], args['s3_eol_contractlines_destination'])
    load_persons(raw_db, args['raw_eol_persons_table'], args['s3_eol_persons_destination'])
    load_divisiondivisiontypes(raw_db, args['raw_eol_divisiondivisiontypes_table'], args['s3_eol_divisiondivisiontypes_destination'])
    load_customersubscriptionstatistics(raw_db, args['raw_eol_customersubscriptionstatistics_table'], args['s3_eol_customersubscriptionstatistics_destination'])
    load_creditmanagementstatus(raw_db, args['raw_eol_creditmanagementstatus_table'], args['s3_eol_creditmanagementstatus_destination'])
    load_divisiontypes(raw_db, args['raw_eol_divisiontypes_table'], args['s3_eol_divisiontypes_destination'])
    load_itemclasses(raw_db, args['raw_eol_itemclasses_table'], args['s3_eol_itemclasses_destination'])
    load_itemrelations(raw_db, args['raw_eol_itemrelations_table'], args['s3_eol_itemrelations_destination'])
    load_leadsources(raw_db, args['raw_eol_leadsources_table'], args['s3_eol_leadsources_destination'])
    load_opportunities_hosting(raw_db, args['raw_eol_opportunities_hosting_table'], args['s3_eol_opportunities_hosting_destination'])
    load_opportunitystages(raw_db, args['raw_eol_opportunitystages_table'], args['s3_eol_opportunitystages_destination'])
    load_saleshandoverdocument(raw_db, args['raw_eol_documents_table'], args['s3_eol_saleshandoverdocument_destination'])
    load_subscriptionquotations(raw_db, args['raw_eol_subscriptionquotations_table'], args['s3_eol_subscriptionquotations_destination'])
    surveyresults_df = load_surveyresults(raw_db, args['raw_eol_surveyresults_table'], args['s3_eol_surveyresults_destination'])
    load_surveyresults_notlinktoaccountant(surveyresults_df, args['s3_eol_surveyresults_notlinktoaccountant_destination'])
    load_units(raw_db, args['raw_eol_units_table'], args['s3_eol_units_destination'])
    load_usagetransactions(raw_db, args['raw_eol_usagetransactions_table'], args['s3_eol_usagetransactions_destination'])
    load_usertypes(raw_db, args['raw_eol_usertypes_table'], args['s3_eol_usertypes_destination'])
    load_userusertypes(raw_db, args['raw_eol_userusertypes_table'], args['s3_eol_userusertypes_destination'])
    load_projects(raw_db, args['raw_eol_projects_table'], args['s3_eol_projects_destination'])
    load_timecosttransactions(raw_db, args['raw_eol_timecosttransactions_table'], args['s3_eol_timecosttransactions_destination'])
    load_usageentitlements(raw_db, args['raw_eol_usageentitlements_table'], args['s3_eol_usageentitlements_destination'])
    load_conversionsource(raw_db, args['raw_eol_conversionstatus_table'], args['raw_eol_gltransactionsources_table'], args['s3_eol_conversionstatus_destination'])
    load_oauthclientslog_view(raw_db, args['raw_eol_oauthclients_table'], args['s3_eol_oauthclientslog_view_destination'])
    load_oauthclients_view(raw_db, args['raw_eol_oauthclients_table'], args['s3_eol_oauthclients_view_destination'])
    load_mail2eol(raw_db, args['raw_eol_mail2eol_table'], args['s3_eol_mail2eol_destination'])
    load_scanprovideruploadlog(raw_db, args['raw_eol_scanprovideruploadlog_table'], args['s3_eol_scanprovideruploadlog_destination'])
    load_scanninglyanthe(raw_db, args['raw_eol_scanninglyanthe_table'], args['s3_eol_scanninglyanthe_destination'])
    load_modules(domain_db, args['domain_contracts_table'], args['domain_accountscontract_summary_table'], args['domain_accounts_table'], args['domain_packageclassification_table'], args['s3_eol_modules_destination'])
    load_divisionstartuptype(args['s3_eol_divisionstartuptype_destination'])
    load_active_user_divisions(raw_db, args['raw_eol_activeuserdivisions_table'], args['s3_eol_activeuserdivisions_destination'])
    load_active_user_roles(raw_db, args['raw_eol_activeuserroles_table'], args['s3_eol_activeuserroles_destination'])
    load_divisiontypefeaturesets(raw_db, args['raw_eol_divisiontypefeaturesets_table'], args['s3_eol_divisiontypefeaturesets_destination'])
