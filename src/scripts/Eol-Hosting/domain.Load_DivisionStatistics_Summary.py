import sys
from datetime import date, timedelta
import time

from utils import *

import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.transforms import ResolveChoice, DropFields

from pyspark.context import SparkContext
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, when, col, upper, trim, lit
from pyspark.sql.session import SparkSession
from pyspark.sql.types import IntegerType, LongType

spark = SparkSession.builder.getOrCreate()
glueContext = GlueContext(SparkContext.getOrCreate())

def load_divisionstatistics_summary(divisionstatistics_raw_df, divisionstatistics_summary_df):
  divisionstatistics_raw_df = divisionstatistics_raw_df.withColumn('environment',upper(trim(col('environment'))))

  if not 'abnamrobanklinksbgwpsd2' in divisionstatistics_raw_df.columns:
      divisionstatistics_raw_df = divisionstatistics_raw_df.withColumn('abnamrobanklinksbgwpsd2', lit(None).cast(LongType()))
  if not 'knabbanklinksbgwsftp' in divisionstatistics_raw_df.columns:
      divisionstatistics_raw_df =  divisionstatistics_raw_df.withColumn('knabbanklinksbgwsftp', lit(None).cast(LongType()))
  if not 'ingbanklinksbgwsftp' in divisionstatistics_raw_df.columns:
      divisionstatistics_raw_df =  divisionstatistics_raw_df.withColumn('ingbanklinksbgwsftp', lit(None).cast(LongType()))
 
  divisionstatistics_raw_df = divisionstatistics_raw_df.withColumn("row_num", row_number().over(Window.partitionBy("environment", "division").orderBy(col("cigcopytime").desc())))
  divisionstatistics_raw_df = divisionstatistics_raw_df.where(col('row_num') == '1')

  divisionstatistics_raw_df = divisionstatistics_raw_df.withColumn("automaticbanklink",
                                        when(col('banklinkscount') > 0, 1)\
                                        .when(col('rabobanklinks') > 0, 1)\
                                        .when(col('abnamrobanklinks') > 0, 1)\
                                        .when(col('ingbanklinks') > 0, 1)\
                                        .otherwise(0)
                                  )
  divisionstatistics_raw_df = divisionstatistics_raw_df.withColumnRenamed('division', 'divisioncode')

  divisionstatistics_raw_df = divisionstatistics_raw_df.drop('row_num')

  divisionstatistics_raw_df= divisionstatistics_raw_df.withColumn('order_num', lit(2).cast(IntegerType()))
  divisionstatistics_summary_df= divisionstatistics_summary_df.withColumn('order_num', lit(1).cast(IntegerType()))

  divisionstatistics_summary_df = divisionstatistics_summary_df.union(divisionstatistics_raw_df)

  divisionstatistics_summary_df = divisionstatistics_summary_df.withColumn("row_num", row_number().over(Window.partitionBy("environment", "divisioncode").orderBy(col("order_num").desc())))
  divisionstatistics_summary_df = divisionstatistics_summary_df.where(col('row_num') == '1')
  divisionstatistics_summary_df = divisionstatistics_summary_df.drop('row_num', 'order_num')
  return divisionstatistics_summary_df

if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, [
      'raw_db', 
      'domain_db', 
      'raw_eol_divisionstatistics_table', 
      'domain_divisionstatistics_summary_table',
      'lastupdated_table',
      's3_bucket',
      's3_destination'
    ])

    session = boto3.Session(region_name='eu-west-1')
    dynamodb = session.resource('dynamodb')
    lastupdated_table = dynamodb.Table(args['lastupdated_table'])

    divisionstatistics_summary_df = glueContext.create_dynamic_frame.from_catalog(database=args['domain_db'], table_name=args['domain_divisionstatistics_summary_table']).toDF()
    divisionstatistics_summary_df.cache()

    last_updated = get_last_updated(lastupdated_table, 'DivisionStatistics_Summary')

    today = date.today()

    difference = (today - last_updated).days

    i = 1
    while i <= difference:
      current_date = last_updated + timedelta(days=i)
      yearfilter = str(current_date.year)
      monthfilter = str(current_date.month)
      dayfilter = str(current_date.day)

      push_down_filter = 'IngestionYear = ' + yearfilter + ' and IngestionMonth = ' + monthfilter + ' and IngestionDay = ' + dayfilter

      divisionstatistics_raw_df = glueContext.create_dynamic_frame.from_catalog(
        database=args['raw_db'], 
        table_name=args['raw_eol_divisionstatistics_table'], 
        push_down_predicate=push_down_filter
      )
      divisionstatistics_raw_df = ResolveChoice.apply(divisionstatistics_raw_df, specs = [
                  ("cashentryavglines", "cast:float"),("bankentryavglines", "cast:float"),("bankentryavglinesimport", "cast:float"),
                  ("bankentryavglinesmanual", "cast:float"),("salesentryavglines", "cast:float"),("purchaseentryavglines", "cast:float"),
                  ("generaljournalentryavglines", "cast:float"),("salesorderentryavglines", "cast:float"),("deliveryentryavglines", "cast:float"),
                  ("stockentryreturnavglines", "cast:float"),("purchaseorderentryavglines", "cast:float"),("receiptentryavglines", "cast:float"),
                  ("receiptreturnavglines", "cast:float"),("stockcountentryavglines", "cast:float"),("psnaveragesubscriptioninvoiceamount", "cast:float"),
                  ("shoporderroutingstepsavglines", "cast:float"),("shopordermaterialsavglines", "cast:float"),("salesinvoiceavglines", "cast:float"),
                  ("salespricelistperiodavgitemcount", "cast:float"),("purchaseinvoiceavglines", "cast:float"),("purchasepricelistperiodavgitemcount", "cast:float"),
                  ("warehousetransferavglines", "cast:float"),("locationtransferavglines", "cast:float"),("smartmatchingwriteoffboundarysetting", "cast:float"),
                  ("batchesperpaymentfileavgcount", "cast:float"),("paymenttransactionsperbatchavgcount", "cast:float"),("division", "cast:long"),
                  ("conversionsource", "cast:long"),("templatedivision", "cast:long"),("taxreturnmethod", "cast:long"),("gltransactionscount", "cast:long"),
                  ("vattransactions", "cast:long"),("fctransactions", "cast:long"),("invoicelinescount", "cast:long"),("banklinkscount", "cast:long"),
                  ("bankaccountscount", "cast:long"),("bankimportfilescount", "cast:long"),("bankexportfilescount", "cast:long"),("rabobankaccounts", "cast:long"),
                  ("rabobanklinks", "cast:long"),("rabobankimportfiles", "cast:long"),("rabobankexportfiles", "cast:long"),("abnamrobankaccounts", "cast:long"),
                  ("abnamrobanklinks", "cast:long"),("abnamrobankstatementfiles", "cast:long"),("ingbankaccounts", "cast:long"),("ingbanklinks", "cast:long"),
                  ("ingbankstatementfiles", "cast:long"),("ingbankpaymentfiles", "cast:long"),("ingbankdirectdebitfiles", "cast:long"),("leadcount", "cast:long"),
                  ("opportunitycount", "cast:long"),("quotecount", "cast:long"),("employeecount", "cast:long"),("accountcount", "cast:long"),
                  ("customercount", "cast:long"),("suppliercount", "cast:long"),("glaccountcount", "cast:long"),("assetcount", "cast:long"),
                  ("allocationrulecount", "cast:long"),("documentcount", "cast:long"),("itemcount", "cast:long"),("journalcount", "cast:long"),
                  ("cashjournalcount", "cast:long"),("bankjournalcount", "cast:long"),("salesjournalcount", "cast:long"),("purchasejournalcount", "cast:long"),
                  ("generaljournalcount", "cast:long"),("cashentrycount", "cast:long"),("cashentrymaxlines", "cast:long"),("cashtransactions", "cast:long"),
                  ("bankentrycount", "cast:long"),("bankentrymaxlines", "cast:long"),("banktransactions", "cast:long"),("bankentrycountimport", "cast:long"),
                  ("bankentrymaxlinesimport", "cast:long"),("banktransactionsimport", "cast:long"),("bankentrycountmanual", "cast:long"),
                  ("bankentrymaxlinesmanual", "cast:long"),("banktransactionsmanual", "cast:long"),("salesentrycount", "cast:long"),("salesentrymaxlines", "cast:long"),
                  ("salestransactions", "cast:long"),("purchaseentrycount", "cast:long"),("purchaseentrymaxlines", "cast:long"),("purchasetransactions", "cast:long"),
                  ("generaljournalentrycount", "cast:long"),("generaljournalentrymaxlines", "cast:long"),("generaljournaltransactions", "cast:long"),
                  ("costcentertransactions", "cast:long"),("costunittransactions", "cast:long"),("depreciationplancount", "cast:long"),
                  ("opportunitystagecount", "cast:long"),("leadsourcecount", "cast:long"),("salestypecount", "cast:long"),("reasoncodecount", "cast:long"),
                  ("budgetcount", "cast:long"),("paymenttermcount", "cast:long"),("sourcenormal", "cast:long"),("sourcebankimport", "cast:long"),
                  ("sourcexmlimport", "cast:long"),("sourceconversion", "cast:long"),("sourceinvoice", "cast:long"),("sourcerevaluation", "cast:long"),
                  ("sourcefixedentry", "cast:long"),("sourcevatreturn", "cast:long"),("sourceglmatching", "cast:long"),("sourceexchangeratediff", "cast:long"),
                  ("fixedsalesentrycount", "cast:long"),("fixedpurchaseentrycount", "cast:long"),("fixedgeneraljournalentrycount", "cast:long"),
                  ("fixedsalesinvoicecount", "cast:long"),("xbrldocumentsbd", "cast:long"),("xbrldocumentskvk", "cast:long"),("projectapproveentries", "cast:long"),
                  ("projectuseproposal", "cast:long"),("projectmandatoryproject", "cast:long"),("projectconsolidatenone", "cast:long"),
                  ("projectconsolidateitem", "cast:long"),("projectconsolidateitemproject", "cast:long"),("projectconsolidateitememployee", "cast:long"),
                  ("projectconsolidateitememployeeproject", "cast:long"),("projecttypefixedprice", "cast:long"),("projecttypetimeandmaterial", "cast:long"),
                  ("projecttypenonbillable", "cast:long"),("projecttotalemployees", "cast:long"),("projecttotaltimeentries", "cast:long"),
                  ("projecttotalcostentries", "cast:long"),("projecttotalabsenceentries", "cast:long"),("projecttotalleaveentries", "cast:long"),
                  ("projecttotaltimecorrectionentries", "cast:long"),("projecttotalsingleplanning", "cast:long"),("projecttotalrecurringplanning", "cast:long"),
                  ("projecttotalnationalholiday", "cast:long"),("projectinvoiceasquoted", "cast:long"),("projecttypeprepaidretainer", "cast:long"),
                  ("projecttypeprepaidhtb", "cast:long"),("projectwithwbs", "cast:long"),("costpricetransactioncount", "cast:long"),("salesorderentrycount", "cast:long"),
                  ("salesorderentrymaxlines", "cast:long"),("deliveryentrycount", "cast:long"),("deliveryentrymaxlines", "cast:long"),("deliverytransactions", "cast:long"),
                  ("stockentryreturncount", "cast:long"),("stockentryreturnmaxlines", "cast:long"),("stockentryreturntransactions", "cast:long"),
                  ("purchaseorderentrycount", "cast:long"),("purchaseorderentrymaxlines", "cast:long"),("receiptentrycount", "cast:long"),
                  ("receiptentrymaxlines", "cast:long"),("receipttransactions", "cast:long"),("receiptreturncount", "cast:long"),("receiptreturnmaxlines", "cast:long"),
                  ("receiptreturntransactions", "cast:long"),("stockcountentrycount", "cast:long"),("stockcountentrymaxlines", "cast:long"),
                  ("stockcounttransactions", "cast:long"),("psnsubscriptontypeautomaticallyrenewedcount", "cast:long"),("psnsubscriptontypemanuallyrenewedcount", "cast:long"),
                  ("psnsubscriptontypenotrenewedcount", "cast:long"),("psnsubscriptionautomaticallyrenewedcount", "cast:long"),
                  ("psnsubscriptionmanuallyrenewedcount", "cast:long"),("psnsubscriptionnotrenewedcount", "cast:long"),("psnsubscriptionyearlycount", "cast:long"),
                  ("psnsubscriptionhalfyearlycount", "cast:long"),("psnsubscriptionquarterlycount", "cast:long"),("psnsubscriptionmonthlycount", "cast:long"),
                  ("psnsubscriptionprintedcount", "cast:long"),("psnsubscriptioninvoicecount", "cast:long"),("payrollactiveemployees", "cast:long"),
                  ("payrollclacount", "cast:long"),("payrolldepartmentcount", "cast:long"),("payrolljobtitlecount", "cast:long"),("payrolldivisiontype", "cast:long"),
                  ("payrollfrequencyyearlyexists", "cast:long"),("payrollfrequencyquarterlyexists", "cast:long"),("payrollfrequencymonthlyexists", "cast:long"),
                  ("payrollfrequency4weeklyexists", "cast:long"),("payrollfrequencyweeklyexists", "cast:long"),("sourceelectronicinvoice", "cast:long"),
                  ("electronicsalesentrycount", "cast:long"),("electronicpurchaseentrycount", "cast:long"),("quotationquantitiescount", "cast:long"),
                  ("quotationmaterialscount", "cast:long"),("quotationroutingstepscount", "cast:long"),("shoporderscount", "cast:long"),
                  ("shopordermaterialscount", "cast:long"),("shoporderroutingstepscount", "cast:long"),("shopordermaterialrealizationscount", "cast:long"),
                  ("shoporderroutingsteprealizationscount", "cast:long"),("shopordermaterialissuescount", "cast:long"),("shopordersubcontractissuescount", "cast:long"),
                  ("shoporderroutingsteptimeentriescount", "cast:long"),("shoporderstockreceiptscount", "cast:long"),("shoporderroutingstepsmaxlines", "cast:long"),
                  ("shopordermaterialsmaxlines", "cast:long"),("shopordersuborderlevelmax", "cast:long"),("budgetscenariocount", "cast:long"),
                  ("costcentercount", "cast:long"),("costunitcount", "cast:long"),("currencycount", "cast:long"),("exchangeratecount", "cast:long"),
                  ("sepabankaccountcount", "cast:long"),("pageviewslastweek", "cast:long"),("pageviewslastmonth", "cast:long"),("backupcount", "cast:long"),
                  ("restorecount", "cast:long"),("backupsinuse", "cast:long"),("backupminbatch", "cast:long"),("backupmaxbatch", "cast:long"),
                  ("inconsistentar", "cast:long"),("inconsistentap", "cast:long"),("inconsistentptmatching", "cast:long"),("inconsistentglmatching", "cast:long"),
                  ("inconsistentbalanceentry", "cast:long"),("inconsistentbalanceperiod", "cast:long"),("inconsistentbalancedate", "cast:long"),
                  ("inconsistentreportingbalance", "cast:long"),("inconsistentbatchitems", "cast:long"),("inconsistentserialitems", "cast:long"),
                  ("inconsistentsalesorderstatus", "cast:long"),("inconsistentpurchaseorderstatus", "cast:long"),("sizekb", "cast:long"),("attachmentsizekb", "cast:long"),
                  ("fulltextwordssizekb", "cast:long"),("backupsizekb", "cast:long"),("applicationlogsizekb", "cast:long"),("gltransactionssizekb", "cast:long"),
                  ("intrastatsales", "cast:long"),("intrastatpurchase", "cast:long"),("mailboxprocessedemailcount", "cast:long"),("mailboxprocessedemailsize", "cast:long"),
                  ("mailboxprocessedattachmentsize", "cast:long"),("requestcount", "cast:long"),("campaigncount", "cast:long"),("marketinglistcount", "cast:long"),
                  ("campaignlinkedtomailchimpcount", "cast:long"),("marketinglistlinkedtomailchimpcount", "cast:long"),("accountlinkedtolinkedincount", "cast:long"),
                  ("contactlinkedtolinkedincount", "cast:long"),("accountlinkedtotwittercount", "cast:long"),("contactlinkedtotwittercount", "cast:long"),
                  ("accountlinkedtoadministrationcount", "cast:long"),("accountlinkedtomultipleadministrationscount", "cast:long"),("indicatorscount", "cast:long"),
                  ("starterusersaccesstocompanycount", "cast:long"),("documentfolderscount", "cast:long"),("documentswithfoldercount", "cast:long"),
                  ("purchasesendercount", "cast:long"),("salessendercount", "cast:long"),("crmsendercount", "cast:long"),("salesinvoicecount", "cast:long"),
                  ("salesinvoicemaxlines", "cast:long"),("dropshipsalesordercount", "cast:long"),("dropshipsuppliersalesordercount", "cast:long"),
                  ("salespricelistcount", "cast:long"),("salespricelistperiodcount", "cast:long"),("salespricelistperiodmaxitemcount", "cast:long"),
                  ("purchaseinvoicecount", "cast:long"),("purchaseinvoicemaxlines", "cast:long"),("purchasepricelistcount", "cast:long"),
                  ("purchasepricelistperiodcount", "cast:long"),("purchasepricelistperiodmaxitemcount", "cast:long"),("batchitemcount", "cast:long"),
                  ("serialitemcount", "cast:long"),("assemblyordercount", "cast:long"),("warehousetransfercount", "cast:long"),("warehousetransfermaxlines", "cast:long"),
                  ("locationtransfercount", "cast:long"),("locationtransfermaxlines", "cast:long"),("useallownegativestock", "cast:long"),
                  ("usesalesorderapproval", "cast:long"),("usetranssmart", "cast:long"),("usemandatoryserialnumber", "cast:long"),("usereceivemorethanordered", "cast:long"),
                  ("userswithchangedcompanyrightscount", "cast:long"),("customerswithinvolveduserscount", "cast:long"),("userssetasinvolvedcount", "cast:long"),
                  ("itemlowestlevelmax", "cast:long"),("employeeswithemployeeratecount", "cast:long"),("hrmapproveleave", "cast:long"),("hrmapproveleavebuildup", "cast:long"),
                  ("projectblockhistoricalentry", "cast:long"),("projectdefaultalertwhenexceeding", "cast:long"),("projectinvoiceproposalreasons", "cast:long"),
                  ("projectnotesmandatory", "cast:long"),("projectrequiresprojectwbs", "cast:long"),("projectsendemails", "cast:long"),
                  ("automatedbankimportsetting", "cast:long"),("glaccountclassificationlinksrgs1", "cast:long"),("glaccountclassificationlinksrgs3", "cast:long"),
                  ("glaccountsuggestionsdirectentrysetting", "cast:long"),("projecttotalleavebuildupentries", "cast:long"),("ctibingbankcount", "cast:long"),("ctibothersbankcount", "cast:long"),
                  ("ctibmatchescount", "cast:long"),("ctibdisabledcount", "cast:long"),("batchesperpaymentfilemaxcount", "cast:long"),("purchaseentrycountforlast90days", "cast:long"),
                  ("electronicpurchaseentrycountforlast90days", "cast:long"),("clientactivitymonthlyexists", "cast:long"),("inactiveaccountcount", "cast:long"),
                  ("anonymizedaccountcount", "cast:long"),("contactcount", "cast:long"),("anonymizedcontactcount", "cast:long"),("inactiveemployeecount", "cast:long"),
                  ("anonymizedemployeecount", "cast:long"),("usercount", "cast:long"),("inactiveusercount", "cast:long"),("anonymizedusercount", "cast:long"),
                  ("scanningserviceautomaticbooking", "cast:long"),("purchaseentryapprovalenabled", "cast:long"),("purchaseentryapproverscount", "cast:long"),
                  ("invoiceproposals", "cast:long"),("projectsalesinvoices", "cast:long"),("mail2eolpurchaseemailallowedsenderscount", "cast:long"),
                  ("mail2eolsalesallowedsenderscount", "cast:long"),("mail2eoldocumentsallowedsenderscount", "cast:long"),("abnamrobanklinksbgwpsd2", "cast:long"),
                  ("knabbanklinksbgwsftp", "cast:long"),("ingbanklinksbgwsftp", "cast:long"),("knabbankaccounts", "cast:long"),("knabbanktransactionfeeds", "cast:long"),
                  ("openingbalancefirstdate", "cast:timestamp"),("lastlogin", "cast:timestamp"),("salesorderlastcreationdate", "cast:timestamp"),
                  ("purchaseorderlastcreationdate", "cast:timestamp"),("shoporderlastcreationdate", "cast:timestamp"),("lastbackup", "cast:timestamp"),("lastrestore", "cast:timestamp"),
                  ("salesinvoicefirstdate", "cast:timestamp"),("consultancydate", "cast:timestamp"),("scanningservicelastusage", "cast:timestamp"),("lastcollected", "cast:timestamp"),
                  ("lastcopied", "cast:timestamp"),("sysmodified", "cast:timestamp"),("syscreated", "cast:timestamp"),("cigcopytime", "cast:timestamp"),("lastcollectedinconsistencies", "cast:timestamp"),
                  ("mail2eolpurchaseemaillastusage", "cast:timestamp"),("mail2eolsalesemaillastusage", "cast:timestamp"),("mail2eoldocumentsemaillastusage", "cast:timestamp")
      ])
      divisionstatistics_raw_df = DropFields.apply(divisionstatistics_raw_df, paths=['freedatefield_01', 'freedatefield_02', 'freedatefield_03',
              'freedatefield_04', 'freedatefield_05', 'freedatefield_06','freedatefield_07', 'freedatefield_08',
              'freedatefield_09', 'freedatefield_10', 'freeintfield_01', 'freeintfield_02','freetextfield_04',
              'freeintfield_03', 'freeintfield_04', 'freeintfield_05', 'freeintfield_06','freetextfield_05',
              'freeintfield_07', 'freeintfield_08', 'freeintfield_09', 'freeintfield_10','freetextfield_03',
              'freeintfield_11', 'freeintfield_12', 'freeintfield_13', 'freeintfield_14','freetextfield_02',
              'freeintfield_15', 'freeintfield_16', 'freeintfield_17', 'freeintfield_18','freetextfield_01',
              'freeintfield_19', 'freeintfield_20'])
      divisionstatistics_raw_df = divisionstatistics_raw_df.toDF()
      divisionstatistics_summary_df = load_divisionstatistics_summary(divisionstatistics_raw_df, divisionstatistics_summary_df)
      i = i + 1

    divisionstatistics_summary_df.repartition(5).write.mode("overwrite").parquet(args['s3_destination']+"_Temp")
    copy_parquets(args["s3_bucket"], "Data/DivisionStatistics_Summary_Temp", "Data/DivisionStatistics_Summary")

    store_last_updated(lastupdated_table, 'DivisionStatistics_Summary', time.mktime(today.timetuple()))