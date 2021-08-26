import sys
import boto3
from utils import *
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import when, col, row_number, lag, lead, lit
from pyspark.sql.session import SparkSession
from datetime import datetime, date, timedelta
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.types import LongType, StringType, DateType, TimestampType, FloatType, LongType, IntegerType

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'raw_db', 'domain_db', 'raw_divisionstatistics_table',
                                     'domain_divisionstatistics_daily_table',
                                     's3_destination'])

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

today = date.today()

yearfilter = str(today.year)
monthfilter = str(today.month)
dayfilter = str(today.day)

yesterday = today - timedelta(days=1)
twodaysago = today - timedelta(days=2)
threedaysago = today - timedelta(days=3)

yesterday_year = str(yesterday.year)
yesterday_month = str(yesterday.month)
yesterday_day = str(yesterday.day)

yeartwo = str(twodaysago.year)
monthtwo = str(twodaysago.month)
daytwo = str(twodaysago.day)

yearthree = str(threedaysago.year)
monththree = str(threedaysago.month)
daythree = str(threedaysago.day)

filterall = 'IngestionYear = ' + yearfilter + ' and IngestionMonth = ' + monthfilter + ' and IngestionDay = ' + dayfilter
glueContext = GlueContext(SparkContext.getOrCreate())

all = glueContext.create_dynamic_frame.from_catalog(database=args['raw_db'],
                                                    table_name=args["raw_divisionstatistics_table"],
                                                    push_down_predicate=filterall).toDF()

raw_df = all.withColumn("cigcopytime", col("cigcopytime").cast("timestamp"))
raw_df = raw_df.withColumn("row_num", row_number().over(
    Window.partitionBy("environment", "division").orderBy(col("cigcopytime").desc())))
raw_df = raw_df.where(col('row_num') == '1')
raw_df = raw_df.withColumnRenamed('division', 'divisioncode')
raw_df = raw_df.withColumn("automaticbanklink",
                           when(col('banklinkscount') > 0, 1).when(col('rabobanklinks') > 0, 1).when(
                               col('abnamrobanklinks') > 0, 1).when(col('ingbanklinks') > 0, 1).otherwise(0))

yesterday_df = raw_df.drop('bankaccountscount', 'bankexportfilescount', 'bankimportfilescount', 'banklinkscount',
                    'cigcopytime', 'cigprocessed', 'consultancydate', 'conversionsource', 'costtyperestrictions',
                    'documentmissingofficialreturncount', 'employeerestrictions', 'freedatefield_01',
                    'freedatefield_02', 'freedatefield_03', 'freedatefield_04', 'freedatefield_05', 'freedatefield_06',
                    'freedatefield_07', 'freedatefield_08', 'freedatefield_09', 'freedatefield_10', 'freeintfield_01',
                    'freeintfield_02', 'freeintfield_03', 'freeintfield_04', 'freeintfield_05', 'freeintfield_06',
                    'freeintfield_07', 'freeintfield_08', 'freeintfield_09', 'freeintfield_10', 'freeintfield_11',
                    'freeintfield_12', 'freeintfield_13', 'freeintfield_14', 'freeintfield_15', 'freeintfield_16',
                    'freeintfield_17', 'freeintfield_18', 'freeintfield_19', 'freeintfield_20', 'freetextfield_01',
                    'freetextfield_02', 'freetextfield_03', 'freetextfield_04', 'freetextfield_05', 'hourtypecount',
                    'hourtyperestrictions', 'ingestionday', 'ingestionmonth', 'ingestionyear',
                    'itemlistextrafieldscount', 'itemtextextrafieldscount', 'lastbackup', 'lastcollected',
                    'lastcollectedinconsistencies', 'lastcopied', 'lastlogin', 'lastrestore',
                    'officialreturnwithduplicatedetailscount', 'openingbalancefirstdate', 'perioddatecheck',
                    'projectconsolidateitememployeeprojectwbs', 'projectconsolidateitemprojectwbs',
                    'projecthourtypebudget', 'projectplanningcommunicationews', 'projectplanningcommunicationoffice365',
                    'psnsubscriptionmidtermchangecount', 'psnsubscriptiononetimefees', 'purchaseentryapprovalenabled',
                    'purchaseentryapproverscount', 'purchaseorderlastcreationdate', 'rebillingrestrictions', 'row_num',
                    'salesinvoicefirstdate', 'salesorderlastcreationdate', 'scanningserviceautomaticbooking',
                    'scanningservicelastusage', 'shoporderlastcreationdate', 'ssfusercount', 'startuptype',
                    'syscreated', 'syscreator', 'sysmodified', 'sysmodifier', 'taxfrequency', 'taxreturnmethod',
                    'taxsystem', 'templatecode', 'templatedivision', 'workcentercount')

yesterday_df = yesterday_df.withColumn('year', lit(yesterday_year))
yesterday_df = yesterday_df.withColumn('month', lit(yesterday_month))
yesterday_df = yesterday_df.withColumn('day', lit(yesterday_day))
yesterday_df = yesterday_df.withColumn('date', lit(str(yesterday)))

time_cols = ['mail2eolpurchaseemaillastusage', 'mail2eolsalesemaillastusage', 'mail2eoldocumentsemaillastusage']

float_cols = ['batchesperpaymentfileavgcount', 'paymenttransactionsperbatchavgcount', 'salesinvoiceavglines',
              'purchaseinvoiceavglines', 'purchasepricelistperiodavgitemcount', 'salespricelistperiodavgitemcount',
              'warehousetransferavglines', 'locationtransferavglines', 'smartmatchingwriteoffboundarysetting',
              'shopordermaterialsavglines', 'shoporderroutingstepsavglines', 'cashentryavglines', 'bankentryavglines',
              'bankentryavglinesimport', 'bankentryavglinesmanual', 'generaljournalentryavglines',
              'psnaveragesubscriptioninvoiceamount', 'stockentryreturnavglines', 'receiptentryavglines',
              'receiptreturnavglines', 'purchaseorderentryavglines', 'stockcountentryavglines', 'salesentryavglines',
              'purchaseentryavglines', 'salesorderentryavglines', 'deliveryentryavglines']

long_cols = ['deliverytransactions', 'stockentryreturncount', 'stockentryreturnmaxlines', 'deliveryentrycount',
             'deliveryentrymaxlines', 'divisioncode', 'purchasetransactions', 'generaljournalentrycount',
             'generaljournalentrymaxlines', 'salestransactions', 'purchaseentrycount', 'purchaseentrymaxlines',
             'projecttotalabsenceentries', 'projecttotalleaveentries', 'projecttotaltimecorrectionentries',
             'projecttotalsingleplanning', 'projecttotalrecurringplanning', 'projecttotalnationalholiday',
             'projectinvoiceasquoted', 'projecttypeprepaidretainer', 'projecttypeprepaidhtb', 'projectwithwbs',
             'costpricetransactioncount', 'salesorderentrycount', 'salesorderentrymaxlines', 'stockcounttransactions',
             'receiptentrycount', 'receiptentrymaxlines', 'receiptreturntransactions', 'stockcountentrycount',
             'stockcountentrymaxlines', 'receipttransactions', 'receiptreturncount', 'receiptreturnmaxlines',
             'stockentryreturntransactions', 'purchaseorderentrycount', 'purchaseorderentrymaxlines',
             'generaljournaltransactions', 'costcentertransactions', 'costunittransactions', 'depreciationplancount',
             'opportunitystagecount', 'leadsourcecount', 'salestypecount', 'reasoncodecount', 'budgetcount',
             'paymenttermcount', 'sourcenormal', 'sourcebankimport', 'sourcexmlimport', 'sourceconversion',
             'sourceinvoice', 'sourcerevaluation', 'sourcefixedentry', 'sourcevatreturn', 'sourceglmatching',
             'sourceexchangeratediff', 'fixedsalesentrycount', 'fixedpurchaseentrycount',
             'fixedgeneraljournalentrycount', 'fixedsalesinvoicecount', 'xbrldocumentsbd', 'xbrldocumentskvk',
             'banktransactionsmanual', 'salesentrycount', 'salesentrymaxlines', 'banktransactionsimport',
             'bankentrycountmanual', 'bankentrymaxlinesmanual', 'banktransactions', 'bankentrycountimport',
             'bankentrymaxlinesimport', 'cashtransactions', 'bankentrycount', 'bankentrymaxlines',
             'gltransactionscount', 'vattransactions', 'fctransactions', 'invoicelinescount', 'automaticbanklink',
             'rabobankaccounts', 'rabobanklinks', 'rabobankimportfiles', 'rabobankexportfiles', 'abnamrobankaccounts',
             'abnamrobanklinks', 'abnamrobankstatementfiles', 'ingbankaccounts', 'ingbanklinks',
             'ingbankstatementfiles', 'ingbankpaymentfiles', 'ingbankdirectdebitfiles', 'leadcount', 'opportunitycount',
             'quotecount', 'employeecount', 'accountcount', 'customercount', 'suppliercount', 'glaccountcount',
             'assetcount', 'allocationrulecount', 'documentcount', 'itemcount', 'journalcount', 'cashjournalcount',
             'bankjournalcount', 'salesjournalcount', 'purchasejournalcount', 'generaljournalcount', 'cashentrycount',
             'cashentrymaxlines', 'shopordermaterialsmaxlines', 'mailboxprocessedemailcount',
             'shopordersuborderlevelmax', 'budgetscenariocount', 'costcentercount', 'costunitcount', 'requestcount',
             'campaigncount', 'marketinglistcount', 'campaignlinkedtomailchimpcount',
             'marketinglistlinkedtomailchimpcount', 'accountlinkedtolinkedincount', 'contactlinkedtolinkedincount',
             'accountlinkedtotwittercount', 'contactlinkedtotwittercount', 'accountlinkedtoadministrationcount',
             'accountlinkedtomultipleadministrationscount', 'indicatorscount', 'starterusersaccesstocompanycount',
             'documentfolderscount', 'documentswithfoldercount', 'purchasesendercount', 'salessendercount',
             'crmsendercount', 'salesinvoicecount', 'sourceelectronicinvoice', 'electronicsalesentrycount',
             'electronicpurchaseentrycount', 'quotationquantitiescount', 'quotationmaterialscount',
             'quotationroutingstepscount', 'shoporderscount', 'shopordermaterialscount', 'shoporderroutingstepscount',
             'shopordermaterialrealizationscount', 'shoporderroutingsteprealizationscount',
             'shopordermaterialissuescount', 'shopordersubcontractissuescount', 'shoporderroutingsteptimeentriescount',
             'shoporderstockreceiptscount', 'shoporderroutingstepsmaxlines', 'currencycount', 'exchangeratecount',
             'sepabankaccountcount', 'pageviewslastweek', 'pageviewslastmonth', 'backupcount', 'restorecount',
             'backupsinuse', 'backupminbatch', 'backupmaxbatch', 'inconsistentar', 'inconsistentap',
             'inconsistentptmatching', 'inconsistentglmatching', 'inconsistentbalanceentry',
             'inconsistentbalanceperiod', 'inconsistentbalancedate', 'inconsistentreportingbalance',
             'inconsistentbatchitems', 'inconsistentserialitems', 'inconsistentsalesorderstatus',
             'inconsistentpurchaseorderstatus', 'projecttotalleavebuildupentries', 'ctibingbankcount',
             'ctibothersbankcount', 'ctibmatchescount', 'ctibdisabledcount', 'locationtransfermaxlines',
             'userswithchangedcompanyrightscount', 'customerswithinvolveduserscount', 'userssetasinvolvedcount',
             'itemlowestlevelmax', 'employeeswithemployeeratecount', 'warehousetransfermaxlines',
             'locationtransfercount', 'salespricelistperiodmaxitemcount', 'purchaseinvoicecount',
             'purchasepricelistperiodmaxitemcount', 'batchitemcount', 'serialitemcount', 'assemblyordercount',
             'warehousetransfercount', 'purchaseinvoicemaxlines', 'purchasepricelistcount',
             'purchasepricelistperiodcount', 'salesinvoicemaxlines', 'dropshipsalesordercount',
             'dropshipsuppliersalesordercount', 'salespricelistcount', 'salespricelistperiodcount',
             'purchaseentrycountforlast90days', 'electronicpurchaseentrycountforlast90days',
             'batchesperpaymentfilemaxcount', 'glaccountclassificationlinksrgs1', 'glaccountclassificationlinksrgs3',
             'inactiveaccountcount', 'anonymizedaccountcount', 'contactcount', 'anonymizedcontactcount',
             'inactiveemployeecount', 'anonymizedemployeecount', 'usercount', 'inactiveusercount',
             'anonymizedusercount', 'invoiceproposals', 'projectsalesinvoices', 'mail2eoldocumentsallowedsenderscount',
             'mail2eolsalesallowedsenderscount', 'mail2eolpurchaseemailallowedsenderscount',
             'clientactivitymonthlyexists', 'intrastatsales', 'intrastatpurchase',
             'glaccountsuggestionsdirectentrysetting', 'hrmapproveleave', 'hrmapproveleavebuildup',
             'projectblockhistoricalentry', 'projectdefaultalertwhenexceeding', 'projectinvoiceproposalreasons',
             'projectnotesmandatory', 'projectrequiresprojectwbs', 'projectsendemails', 'automatedbankimportsetting',
             'useallownegativestock', 'usesalesorderapproval', 'usetranssmart', 'usemandatoryserialnumber',
             'usereceivemorethanordered', 'payrolldivisiontype', 'payrollfrequencyyearlyexists',
             'payrollfrequencyquarterlyexists', 'payrollfrequencymonthlyexists', 'payrollfrequency4weeklyexists',
             'payrollfrequencyweeklyexists', 'projectapproveentries', 'projectuseproposal', 'projectmandatoryproject',
             'projectconsolidatenone', 'projectconsolidateitem', 'projectconsolidateitemproject',
             'projectconsolidateitememployee', 'projectconsolidateitememployeeproject', 'projecttypefixedprice',
             'projecttypetimeandmaterial', 'projecttypenonbillable', 'projecttotalemployees', 'projecttotaltimeentries',
             'projecttotalcostentries', 'psnsubscriptontypeautomaticallyrenewedcount',
             'psnsubscriptontypemanuallyrenewedcount', 'psnsubscriptontypenotrenewedcount',
             'psnsubscriptionautomaticallyrenewedcount', 'psnsubscriptionmanuallyrenewedcount',
             'psnsubscriptionnotrenewedcount', 'psnsubscriptionyearlycount', 'psnsubscriptionhalfyearlycount',
             'psnsubscriptionquarterlycount', 'psnsubscriptionmonthlycount', 'psnsubscriptionprintedcount',
             'psnsubscriptioninvoicecount', 'payrollactiveemployees', 'payrollclacount', 'payrolldepartmentcount',
             'payrolljobtitlecount', 'sizekb', 'attachmentsizekb', 'fulltextwordssizekb', 'backupsizekb',
             'applicationlogsizekb', 'gltransactionssizekb', 'mailboxprocessedemailsize',
             'mailboxprocessedattachmentsize']

integer_cols = ['day', 'month', 'year']

yesterday_df = yesterday_df.withColumn('date', col('date').cast(DateType()))

yesterday_df = convertDataTypes(
    data_frame=yesterday_df,
    timestamp_cols=time_cols,
    integer_cols = integer_cols,
    float_cols = float_cols,
    long_cols = long_cols
)

output_path = args['s3_destination'] + '/Year=' + yesterday_year + '/Month=' + yesterday_month + '/Day=' + yesterday_day
yesterday_df = yesterday_df.drop('year','month','day')
yesterday_df.repartition(1).write.mode("overwrite").parquet(output_path)

#SECOND PART
filtertwo = 'Year = ' + yeartwo + ' and Month = ' + monthtwo + ' and Day = ' + daytwo # 2 days ago data
filterthree = 'Year = ' + yearthree + ' and Month = ' + monththree + ' and Day = ' + daythree # Yesterday's data

threedays = glueContext.create_dynamic_frame.from_catalog(database = "customerintelligence", table_name ='divisionstatistics_daily',
                                                       push_down_predicate=filterthree).toDF()
twodays = glueContext.create_dynamic_frame.from_catalog(database = "customerintelligence", table_name ='divisionstatistics_daily',
                                                        push_down_predicate=filtertwo).toDF()

cols = ['environment', 'divisioncode', 'date', 'gltransactionscount', 'vattransactions', 'fctransactions',
        'invoicelinescount', 'automaticbanklink', 'rabobankaccounts', 'rabobanklinks', 'rabobankimportfiles',
        'rabobankexportfiles', 'abnamrobankaccounts', 'abnamrobanklinks', 'abnamrobankstatementfiles',
        'ingbankaccounts', 'ingbanklinks', 'ingbankstatementfiles', 'ingbankpaymentfiles', 'ingbankdirectdebitfiles',
        'leadcount', 'opportunitycount', 'quotecount', 'employeecount', 'accountcount', 'customercount',
        'suppliercount', 'glaccountcount', 'assetcount', 'allocationrulecount', 'documentcount', 'itemcount',
        'journalcount', 'cashjournalcount', 'bankjournalcount', 'salesjournalcount', 'purchasejournalcount',
        'generaljournalcount', 'cashentrycount', 'cashentrymaxlines', 'cashentryavglines', 'cashtransactions',
        'bankentrycount', 'bankentrymaxlines', 'bankentryavglines', 'banktransactions', 'bankentrycountimport',
        'bankentrymaxlinesimport', 'bankentryavglinesimport', 'banktransactionsimport', 'bankentrycountmanual',
        'bankentrymaxlinesmanual', 'bankentryavglinesmanual', 'banktransactionsmanual', 'salesentrycount',
        'salesentrymaxlines', 'salesentryavglines', 'salestransactions', 'purchaseentrycount', 'purchaseentrymaxlines',
        'purchaseentryavglines', 'purchasetransactions', 'generaljournalentrycount', 'generaljournalentrymaxlines',
        'generaljournalentryavglines', 'generaljournaltransactions', 'costcentertransactions', 'costunittransactions',
        'depreciationplancount', 'opportunitystagecount', 'leadsourcecount', 'salestypecount', 'reasoncodecount',
        'budgetcount', 'paymenttermcount', 'sourcenormal', 'sourcebankimport', 'sourcexmlimport', 'sourceconversion',
        'sourceinvoice', 'sourcerevaluation', 'sourcefixedentry', 'sourcevatreturn', 'sourceglmatching',
        'sourceexchangeratediff', 'fixedsalesentrycount', 'fixedpurchaseentrycount', 'fixedgeneraljournalentrycount',
        'fixedsalesinvoicecount', 'xbrldocumentsbd', 'xbrldocumentskvk', 'projectapproveentries', 'projectuseproposal',
        'projectmandatoryproject', 'projectconsolidatenone', 'projectconsolidateitem', 'projectconsolidateitemproject',
        'projectconsolidateitememployee', 'projectconsolidateitememployeeproject', 'projecttypefixedprice',
        'projecttypetimeandmaterial', 'projecttypenonbillable', 'projecttotalemployees', 'projecttotaltimeentries',
        'projecttotalcostentries', 'projecttotalabsenceentries', 'projecttotalleaveentries',
        'projecttotaltimecorrectionentries', 'projecttotalsingleplanning', 'projecttotalrecurringplanning',
        'projecttotalnationalholiday', 'projectinvoiceasquoted', 'projecttypeprepaidretainer', 'projecttypeprepaidhtb',
        'projectwithwbs', 'costpricetransactioncount', 'salesorderentrycount', 'salesorderentrymaxlines',
        'salesorderentryavglines', 'deliveryentrycount', 'deliveryentrymaxlines', 'deliveryentryavglines',
        'deliverytransactions', 'stockentryreturncount', 'stockentryreturnmaxlines', 'stockentryreturnavglines',
        'stockentryreturntransactions', 'purchaseorderentrycount', 'purchaseorderentrymaxlines',
        'purchaseorderentryavglines', 'receiptentrycount', 'receiptentrymaxlines', 'receiptentryavglines',
        'receipttransactions', 'receiptreturncount', 'receiptreturnmaxlines', 'receiptreturnavglines',
        'receiptreturntransactions', 'stockcountentrycount', 'stockcountentrymaxlines', 'stockcountentryavglines',
        'stockcounttransactions', 'psnsubscriptontypeautomaticallyrenewedcount',
        'psnsubscriptontypemanuallyrenewedcount', 'psnsubscriptontypenotrenewedcount',
        'psnsubscriptionautomaticallyrenewedcount', 'psnsubscriptionmanuallyrenewedcount',
        'psnsubscriptionnotrenewedcount', 'psnsubscriptionyearlycount', 'psnsubscriptionhalfyearlycount',
        'psnsubscriptionquarterlycount', 'psnsubscriptionmonthlycount', 'psnsubscriptionprintedcount',
        'psnsubscriptioninvoicecount', 'psnaveragesubscriptioninvoiceamount', 'contracttype', 'payrollactiveemployees',
        'payrollclacount', 'payrolldepartmentcount', 'payrolljobtitlecount', 'payrolldivisiontype',
        'payrollfrequencyyearlyexists', 'payrollfrequencyquarterlyexists', 'payrollfrequencymonthlyexists',
        'payrollfrequency4weeklyexists', 'payrollfrequencyweeklyexists', 'sourceelectronicinvoice',
        'electronicsalesentrycount', 'electronicpurchaseentrycount', 'quotationquantitiescount',
        'quotationmaterialscount', 'quotationroutingstepscount', 'shoporderscount', 'shopordermaterialscount',
        'shoporderroutingstepscount', 'shopordermaterialrealizationscount', 'shoporderroutingsteprealizationscount',
        'shopordermaterialissuescount', 'shopordersubcontractissuescount', 'shoporderroutingsteptimeentriescount',
        'shoporderstockreceiptscount', 'shoporderroutingstepsmaxlines', 'shoporderroutingstepsavglines',
        'shopordermaterialsmaxlines', 'shopordermaterialsavglines', 'shopordersuborderlevelmax', 'budgetscenariocount',
        'costcentercount', 'costunitcount', 'foreigncurrency', 'currencycount', 'exchangeratecount',
        'sepabankaccountcount', 'pageviewslastweek', 'pageviewslastmonth', 'backupcount', 'restorecount',
        'backupsinuse', 'backupminbatch', 'backupmaxbatch', 'inconsistentar', 'inconsistentap',
        'inconsistentptmatching', 'inconsistentglmatching', 'inconsistentbalanceentry', 'inconsistentbalanceperiod',
        'inconsistentbalancedate', 'inconsistentreportingbalance', 'inconsistentbatchitems', 'inconsistentserialitems',
        'inconsistentsalesorderstatus', 'inconsistentpurchaseorderstatus', 'sizekb', 'attachmentsizekb',
        'fulltextwordssizekb', 'backupsizekb', 'applicationlogsizekb', 'gltransactionssizekb', 'intrastatsales',
        'intrastatpurchase', 'mailboxconnectiontype', 'mailboxprocessedemailcount', 'mailboxprocessedemailsize',
        'mailboxprocessedattachmentsize', 'requestcount', 'campaigncount', 'marketinglistcount',
        'campaignlinkedtomailchimpcount', 'marketinglistlinkedtomailchimpcount', 'accountlinkedtolinkedincount',
        'contactlinkedtolinkedincount', 'accountlinkedtotwittercount', 'contactlinkedtotwittercount',
        'accountlinkedtoadministrationcount', 'accountlinkedtomultipleadministrationscount', 'indicatorscount',
        'starterusersaccesstocompanycount', 'documentfolderscount', 'documentswithfoldercount', 'purchasesendercount',
        'salessendercount', 'crmsendercount', 'salesinvoicecount', 'salesinvoiceavglines', 'salesinvoicemaxlines',
        'dropshipsalesordercount', 'dropshipsuppliersalesordercount', 'salespricelistcount',
        'salespricelistperiodcount', 'salespricelistperiodavgitemcount', 'salespricelistperiodmaxitemcount',
        'purchaseinvoicecount', 'purchaseinvoiceavglines', 'purchaseinvoicemaxlines', 'purchasepricelistcount',
        'purchasepricelistperiodcount', 'purchasepricelistperiodavgitemcount', 'purchasepricelistperiodmaxitemcount',
        'batchitemcount', 'serialitemcount', 'assemblyordercount', 'warehousetransfercount',
        'warehousetransferavglines', 'warehousetransfermaxlines', 'locationtransfercount', 'locationtransferavglines',
        'locationtransfermaxlines', 'useallownegativestock', 'usesalesorderapproval', 'usetranssmart',
        'usemandatoryserialnumber', 'usereceivemorethanordered', 'userswithchangedcompanyrightscount',
        'customerswithinvolveduserscount', 'userssetasinvolvedcount', 'itemlowestlevelmax',
        'employeeswithemployeeratecount', 'hrmapproveleave', 'hrmapproveleavebuildup', 'projectblockhistoricalentry',
        'projectdefaultalertwhenexceeding', 'projectinvoiceproposalreasons', 'projectnotesmandatory',
        'projectrequiresprojectwbs', 'projectsendemails', 'automatedbankimportsetting',
        'glaccountclassificationlinksrgs1', 'glaccountclassificationlinksrgs3',
        'glaccountsuggestionsdirectentrysetting', 'smartmatchingwriteoffboundarysetting',
        'projecttotalleavebuildupentries', 'ctibingbankcount', 'ctibothersbankcount', 'ctibmatchescount',
        'ctibdisabledcount', 'batchesperpaymentfileavgcount', 'batchesperpaymentfilemaxcount',
        'paymenttransactionsperbatchavgcount', 'purchaseentrycountforlast90days',
        'electronicpurchaseentrycountforlast90days', 'clientactivitymonthlyexists', 'inactiveaccountcount',
        'anonymizedaccountcount', 'contactcount', 'anonymizedcontactcount', 'inactiveemployeecount',
        'anonymizedemployeecount', 'usercount', 'inactiveusercount', 'anonymizedusercount', 'invoiceproposals',
        'projectsalesinvoices', 'mail2eolpurchaseemail', 'mail2eolpurchaseemaillastusage',
        'mail2eolpurchaseemailallowedsenderscount', 'mail2eolsalesemail', 'mail2eolsalesemaillastusage',
        'mail2eolsalesallowedsenderscount', 'mail2eoldocumentsemail', 'mail2eoldocumentsemaillastusage',
        'mail2eoldocumentsallowedsenderscount', 'Year', 'Month', 'Day']

yesterday_df = yesterday_df.withColumn('year', lit(yesterday_year))
yesterday_df = yesterday_df.withColumn('month', lit(yesterday_month))
yesterday_df = yesterday_df.withColumn('day', lit(yesterday_day))
yesterday_df = yesterday_df.withColumn('date', lit(str(yesterday)))
yesterday_df = yesterday_df.select(cols)

# union last 3 days
def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)

last3days = unionAll(yesterday_df, twodays,threedays)

last3days = last3days.fillna({'gltransactionscount': '0'})
last3days = last3days.withColumn('previous', lag(col('gltransactionscount')).over(
    Window.partitionBy("environment", "divisioncode").orderBy(col("date").desc())))
last3days = last3days.withColumn('next', lead(col('gltransactionscount')).over(
    Window.partitionBy("environment", "divisioncode").orderBy(col("date").desc())))

laglead = last3days.withColumn('MinusLead', (col('gltransactionscount') - col('previous')))
laglead = laglead.withColumn('MinusLag', (col('next') - col('gltransactionscount')))

# Create dataframe that needs to be deleted
df_filter = laglead.where((col('gltransactionscount') == '0') & (col('MinusLead') < 0) & (col('MinusLag') > 0) & (
    col('vattransactions').isNull()) & ((col('accountcount') == 0) | (col('accountcount').isNull())))

twodays = twodays.alias('twodays')
df_filter = df_filter.alias('df_filter')

# Join filter to delete null records
final_two_days_df = twodays.join(df_filter, ['divisioncode', 'environment', 'date'], "left_outer").where(
    df_filter.divisioncode.isNull()).select('twodays.*')
final_two_days_df = final_two_days_df.drop('year','month','day')

final_two_days_output_path = args['s3_destination'] + '/Year=' + yeartwo + '/Month=' + monthtwo + '/Day=' + daytwo
final_two_days_df.repartition(1).write.mode("overwrite").parquet(final_two_days_output_path+'/_Temp')

source_key ='Data/DivisionStatistics_Daily/Year='+yeartwo+'/Month='+monthtwo+'/Day='+daytwo+'/_Temp'
destination_key ='Data/DivisionStatistics_Daily/Year='+yeartwo+'/Month='+monthtwo+'/Day='+daytwo
copy_parquets('cig-prod-domain-bucket', source_key, destination_key)