resource "aws_s3_bucket_object" "Load_DivisionStatistics_DailyChanges" {
  bucket = "${var.glue_artifact_store}"
  key    = "Glue/${var.environment}/Eol-Hosting/domain.Load_DivisionStatistics_DailyChanges.py"
  source = "scripts/Eol-Hosting/domain.Load_DivisionStatistics_DailyChanges.py"
  etag   = "${md5(file("scripts/Eol-Hosting/domain.Load_DivisionStatistics_DailyChanges.py"))}"
}

resource "aws_glue_job" "Load_DivisionStatistics_DailyChanges" {
  name               = "domain.Load_DivisionStatistics_DailyChanges"
  role_arn           = "${data.aws_iam_role.cig_glue_role.arn}"
  allocated_capacity = "${var.dpu}"

  command {
    script_location = "s3://${var.glue_artifact_store}/${aws_s3_bucket_object.Load_DivisionStatistics_DailyChanges.id}"
  }

  default_arguments = {
    "--domain_db"                      = "${var.domain_database}"
    "--divisionstatistics_daily_table" = "${var.domain_divisionstatistics_daily_table}"
    "--s3_destination"                 = "s3://${var.domain_bucket}/Data/DivisionStatistics_DailyChanges"
    "--job-bookmark-option"            = "${var.bookmark}"
  }
}

resource "aws_s3_bucket_object" "Load_DivisionStatistics_Summary" {
  bucket = "${var.glue_artifact_store}"
  key    = "Glue/${var.environment}/Eol-Hosting/Load_DivisionStatistics_Summary.py"
  source = "scripts/Eol-Hosting/domain.Load_DivisionStatistics_Summary.py"
  etag   = "${md5(file("scripts/Eol-Hosting/domain.Load_DivisionStatistics_Summary.py"))}"
}

resource "aws_glue_job" "Load_DivisionStatistics_Summary" {
  name               = "domain.Load_DivisionStatistics_Summary"
  role_arn           = "${data.aws_iam_role.cig_glue_role.arn}"
  allocated_capacity = "${var.dpu}"

  command {
    script_location = "s3://${var.glue_artifact_store}/${aws_s3_bucket_object.Load_DivisionStatistics_Summary.id}"
  }

  default_arguments = {
    "--extra-py-files"                           = "s3://${var.glue_artifact_store}/${aws_s3_bucket_object.Utils.id}"
    "--raw_db"                                   = "${var.raw_database}"
    "--domain_db"                                = "${var.domain_database}"
    "--raw_eol_divisionstatistics_table"          = "${var.raw_eol_divisionstatistics_table}"
    "--domain_divisionstatistics_summary_table"  = "${var.domain_divisionstatistics_summary_table}"
    "--lastupdated_table"                        = "${aws_dynamodb_table.lastupdated_table.id}"
    "--s3_bucket"                                = "${var.domain_bucket}"
    "--s3_destination"                           = "s3://${var.domain_bucket}/Data/DivisionStatistics_Summary"
  }
}

resource "aws_s3_bucket_object" "Load_DivisionStatistics_Daily" {
  bucket = "${var.glue_artifact_store}"
  key    = "Glue/${var.environment}/Eol-Hosting/domain.Load_DivisionStatistics_Daily.py"
  source = "scripts/Eol-Hosting/domain.Load_DivisionStatistics_Daily.py"
  etag   = "${md5(file("scripts/Eol-Hosting/domain.Load_DivisionStatistics_Daily.py"))}"
}

resource "aws_glue_job" "Load_DivisionStatistics_Daily" {
  name               = "domain.Load_DivisionStatistics_Daily"
  role_arn           = "${data.aws_iam_role.cig_glue_role.arn}"
  allocated_capacity = "${var.dpu}"

  command {
    script_location = "s3://${var.glue_artifact_store}/${aws_s3_bucket_object.Load_DivisionStatistics_Daily.id}"
  }

  default_arguments = {
    "--extra-py-files"                        = "s3://${var.glue_artifact_store}/${aws_s3_bucket_object.Utils.id}"
    "--raw_db"                                = "${var.raw_database}"
    "--domain_db"                             = "${var.domain_database}"
    "--raw_divisionstatistics_table"          = "${var.raw_eol_divisionstatistics_table}"
    "--domain_divisionstatistics_daily_table" = "${var.domain_divisionstatistics_daily_table}"
    "--s3_destination"                        = "s3://${var.domain_bucket}/Data/DivisionStatistics_Daily"
    "--job-bookmark-option"                   = "${var.bookmark}"
  }
}

resource "aws_s3_bucket_object" "Transformations" {
  bucket = "${var.glue_artifact_store}"
  key    = "Glue/${var.environment}/Eol-Hosting/domain.Transformations.py"
  source = "scripts/Eol-Hosting/domain.Transformations.py"
  etag   = "${md5(file("scripts/Eol-Hosting/domain.Transformations.py"))}"
}

resource "aws_s3_bucket_object" "RawTransformations" {
  bucket = "${var.glue_artifact_store}"
  key    = "Glue/${var.environment}/Eol-Hosting/raw.Transformations.py"
  source = "scripts/Eol-Hosting/raw.Transformations.py"
  etag   = "${md5(file("scripts/Eol-Hosting/raw.Transformations.py"))}"
}

resource "aws_glue_job" "RawTransformations" {
  name = "raw.Load_EolHosting_MultipleRawTransformations"
  role_arn = "${data.aws_iam_role.cig_glue_role.arn}"
  allocated_capacity = "${var.dpu}"

  command {
    script_location = "s3://${var.glue_artifact_store}/${aws_s3_bucket_object.RawTransformations.id}"
  }

  default_arguments = {
    "--extra-py-files"                                    = "s3://${var.glue_artifact_store}/${aws_s3_bucket_object.Utils.id}"
    "--raw_db"                                            = "${var.raw_database}"
    "--lastupdated_table"                                 = "${aws_dynamodb_table.lastupdated_table.id}"
    "--raw_eol_contracts_table"                           = "${var.raw_eol_contracts_table}"
    "--s3_eol_raw_contracts_destination"                  = "s3://${var.raw_bucket}/Data/Contracts"
    "--raw_eol_accounts_table"                            = "${var.raw_eol_accounts_table}"
    "--s3_eol_raw_accounts_destination"                   = "s3://${var.raw_bucket}/Data/Accounts"
    "--s3_domain_eol_accounts_hosting_view_destination"   = "s3://${var.domain_bucket}/Data/Accounts_Hosting_View"
    "--raw_eol_paymentterms_table"                        = "${var.raw_eol_paymentterms_table}"
    "--s3_eol_raw_paymentterms_destination"               = "s3://${var.raw_bucket}/Data/PaymentTerms"
    "--raw_eol_persons_table"                             = "${var.raw_eol_persons_table}"
    "--s3_eol_raw_persons_email_destination"              = "s3://${var.raw_bucket}/Data/Persons_Email"
    "--raw_eol_users_table"                               = "${var.raw_eol_users_table}"
    "--s3_eol_raw_users_googleclientid_destination"       = "s3://${var.raw_bucket}/Data/Users_GooogleClientId"
    "--s3_eol_contracts_new_view_destination"             = "s3://${var.domain_bucket}/Data/Contracts_New_View"
    "--s3_config_gdpr_accountsdeletionlog_destination"    = "s3://${var.domain_bucket}/Data/Config.GDPR_AccountsDeletionLog"
    "--s3_config_gdpr_personsdeletionlog_destination"    = "s3://${var.domain_bucket}/Data/Config.GDPR_PersonsDeletionLog"
    "--job-bookmark-option"                               = "${var.bookmark}"
  }
}

resource "aws_s3_bucket_object" "KeyEventsTransformations" {
  bucket = "${var.glue_artifact_store}"
  key    = "Glue/${var.environment}/Eol-Hosting/domain.Load_KeyEvents.py"
  source = "scripts/Eol-Hosting/domain.Load_KeyEvents.py"
  etag   = "${md5(file("scripts/Eol-Hosting/domain.Load_KeyEvents.py"))}"
}

resource "aws_glue_job" "KeyEventsTransformations" {
  name = "domain.Load_EolHosting_KeyEventsTransformations"
  role_arn = "${data.aws_iam_role.cig_glue_role.arn}"
  allocated_capacity = "${var.dpu}"

  command {
    script_location = "s3://${var.glue_artifact_store}/${aws_s3_bucket_object.KeyEventsTransformations.id}"
  }

  default_arguments = {
    "--extra-py-files"                          = "s3://${var.glue_artifact_store}/${aws_s3_bucket_object.Utils.id}"
    "--raw_db"                                  = "${var.raw_database}"
    "--domain_db"                               = "${var.domain_database}"
    "--s3_bucket"                               = "${var.domain_bucket}"
    "--domain_keyevents_all_table"              = "${var.domain_keyevents_all_table}"
    "--domain_divisions_table"                  = "${var.domain_divisions_table}"
    "--domain_divisionstatistics_daily_table"   = "${var.domain_divisionstatistics_daily_table}"
    "--raw_eol_divisionstatistics_table"        = "${var.raw_eol_divisionstatistics_table}"
    "--domain_accountscontract_summary_table"   = "${var.domain_accountscontract_summary_table}"
    "--domain_accounts_table"                   = "${var.domain_accounts_table}"
    "--domain_timecosttransactions_table"       = "${var.domain_timecosttransactions_table}"
    "--domain_items_table"                      = "${var.domain_items_table}"
    "--domain_activitydaily_table"              = "${var.domain_activitydaily_table}"
    "--domain_requests_consultaanvraag_table"   = "${var.domain_requests_consultaanvraag_table}"
    "--domain_divisionstatistics_summary_table" = "${var.domain_divisionstatistics_summary_table}"
    "--domain_linkedaccountantlog_table"        = "${var.domain_linkedaccountantlog_table}"
    "--lastupdated_table"                       = "${aws_dynamodb_table.lastupdated_table.id}"
    "--s3_destination"                          = "s3://${var.domain_bucket}/Data/KeyEvents_All"
  }
}

resource "aws_s3_bucket_object" "GDPRTransformations" {
  bucket = "${var.glue_artifact_store}"
  key    = "Glue/${var.environment}/Eol-Hosting/gdpr.Transformations.py"
  source = "scripts/Eol-Hosting/gdpr.Transformations.py"
  etag   = "${md5(file("scripts/Eol-Hosting/gdpr.Transformations.py"))}"
}

resource "aws_glue_job" "GDPRTransformations" {
  name = "gdpr.GDPR_EolHosting_MultipleTransformations"
  role_arn = "${data.aws_iam_role.cig_glue_role.arn}"
  allocated_capacity = "${var.dpu}"

  command {
    script_location = "s3://${var.glue_artifact_store}/${aws_s3_bucket_object.GDPRTransformations.id}"
  }

  default_arguments = {
    "--extra-py-files"                                    = "s3://${var.glue_artifact_store}/${aws_s3_bucket_object.Utils.id}"
    "--raw_db"                                            = "${var.raw_database}"
    "--raw_bucket"                                        = "${var.raw_bucket}"
    "--athena_output_location"                            = "s3://${var.athena_output_bucket}/GDPRTransformations"
    "--athena_output_bucket"                              = "${var.athena_output_bucket}"
    "--lastupdated_table"                                 = "${aws_dynamodb_table.lastupdated_table.id}"
    "--job-bookmark-option"                               = "${var.bookmark}"
  }
}

resource "aws_s3_bucket_object" "Utils" {
  bucket = "${var.glue_artifact_store}"
  key    = "Glue/${var.environment}/Eol-Hosting/utils.py"
  source = "scripts/utils.py"
  etag   = "${md5(file("scripts/utils.py"))}"
}

resource "aws_glue_job" "DomainTransformations" {
  name               = "domain.Load_EolHosting_MultipleDomainTransformations"
  role_arn           = "${data.aws_iam_role.cig_glue_role.arn}"
  allocated_capacity = "${var.dpu}"

  command {
    script_location = "s3://${var.glue_artifact_store}/${aws_s3_bucket_object.Transformations.id}"
  }

  default_arguments = {
    "--extra-py-files"                                          = "s3://${var.glue_artifact_store}/${aws_s3_bucket_object.Utils.id}"
    "--raw_db"                                                  = "${var.raw_database}"
    "--domain_db"                                               = "${var.domain_database}"
    "--raw_eol_items_table"                                     = "${var.raw_eol_items_table}"
    "--s3_eol_items_destination"                                = "s3://${var.domain_bucket}/Data/Items"
    "--raw_eol_contacts_table"                                  = "${var.raw_eol_contacts_table}"
    "--s3_eol_contacts_destination"                             = "s3://${var.domain_bucket}/Data/Contacts"
    "--raw_eol_divisions_table"                                 = "${var.raw_eol_divisions_table}"
    "--s3_eol_divisions_destination"                            = "s3://${var.domain_bucket}/Data/Divisions"
    "--raw_eol_activitysectors_table"                           = "${var.raw_eol_activitysectors_table}"
    "--s3_eol_activitysectors_destination"                      = "s3://${var.domain_bucket}/Data/ActivitySectors"
    "--raw_eol_companysizes_table"                              = "${var.raw_eol_companysizes_table}"
    "--s3_eol_companysizes_destination"                         = "s3://${var.domain_bucket}/Data/CompanySize"
    "--raw_eol_users_table"                                     = "${var.raw_eol_users_table}"
    "--s3_eol_users_destination"                                = "s3://${var.domain_bucket}/Data/Users"
    "--raw_eol_appusagelines_table"                             = "${var.raw_eol_appusagelines_table}"
    "--s3_eol_appusagelines_destination"                        = "s3://${var.domain_bucket}/Data/AppUsageLines"
    "--raw_eol_blockingstatus_table"                            = "${var.raw_eol_blockingstatus_table}"
    "--s3_eol_blockingstatus_destination"                       = "s3://${var.domain_bucket}/Data/BlockingStatus"
    "--raw_eol_businesstypes_table"                             = "${var.raw_eol_businesstypes_table}"
    "--s3_eol_businesstypes_destination"                        = "s3://${var.domain_bucket}/Data/BusinessTypes"
    "--raw_eol_contractmutations_table"                         = "${var.raw_eol_contractmutations_table}"
    "--s3_eol_contractmutations_destination"                    = "s3://${var.domain_bucket}/Data/ContractMutations"
    "--raw_eol_contractevents_table"                            = "${var.raw_eol_contractevents_table}"
    "--s3_eol_contractevents_destination"                       = "s3://${var.domain_bucket}/Data/ContractEvents"
    "--raw_eol_accountsclassifications_table"                   = "${var.raw_eol_accountsclassifications_table}"
    "--s3_eol_accountsclassifications_destination"              = "s3://${var.domain_bucket}/Data/AccountsClassifications"
    "--raw_eol_contractlines_table"                             = "${var.raw_eol_contractlines_table}"
    "--s3_eol_contractlines_destination"                        = "s3://${var.domain_bucket}/Data/ContractLines"
    "--raw_eol_persons_table"                                   = "${var.raw_eol_persons_table}"
    "--s3_eol_persons_destination"                              = "s3://${var.domain_bucket}/Data/Persons"
    "--raw_eol_divisiondivisiontypes_table"                     = "${var.raw_eol_divisiondivisiontypes_table}"
    "--s3_eol_divisiondivisiontypes_destination"                = "s3://${var.domain_bucket}/Data/DivisionDivisionTypes"
    "--raw_eol_creditmanagementstatus_table"                    = "${var.raw_eol_creditmanagementstatus_table}"
    "--s3_eol_creditmanagementstatus_destination"               = "s3://${var.domain_bucket}/Data/CreditManagementStatus"
    "--raw_eol_customersubscriptionstatistics_table"            = "${var.raw_eol_customersubscriptionstatistics_table}"
    "--s3_eol_customersubscriptionstatistics_destination"       = "s3://${var.domain_bucket}/Data/CustomerSubscriptionStatistics"
    "--raw_eol_divisiontypes_table"                             = "${var.raw_eol_divisiontypes_table}"
    "--s3_eol_divisiontypes_destination"                        = "s3://${var.domain_bucket}/Data/DivisionTypes"
    "--raw_eol_itemclasses_table"                               = "${var.raw_eol_itemclasses_table}"
    "--s3_eol_itemclasses_destination"                          = "s3://${var.domain_bucket}/Data/ItemClasses"
    "--raw_eol_itemrelations_table"                             = "${var.raw_eol_itemrelations_table}"
    "--s3_eol_itemrelations_destination"                        = "s3://${var.domain_bucket}/Data/ItemRelations"
    "--raw_eol_leadsources_table"                               = "${var.raw_eol_leadsources_table}"
    "--s3_eol_leadsources_destination"                          = "s3://${var.domain_bucket}/Data/LeadSources"
    "--raw_eol_opportunities_hosting_table"                     = "${var.raw_eol_opportunities_hosting_table}"
    "--s3_eol_opportunities_hosting_destination"                = "s3://${var.domain_bucket}/Data/Opportunities_Hosting"
    "--raw_eol_opportunitystages_table"                         = "${var.raw_eol_opportunitystages_table}"
    "--s3_eol_opportunitystages_destination"                    = "s3://${var.domain_bucket}/Data/OpportunityStages"
    "--raw_eol_documents_table"                                 = "${var.raw_eol_documents_table}"
    "--s3_eol_saleshandoverdocument_destination"                = "s3://${var.domain_bucket}/Data/SalesHandoverDocument"
    "--raw_eol_subscriptionquotations_table"                    = "${var.raw_eol_subscriptionquotations_table}"
    "--s3_eol_subscriptionquotations_destination"               = "s3://${var.domain_bucket}/Data/SubscriptionQuotations"
    "--raw_eol_surveyresults_table"                             = "${var.raw_eol_surveyresults_table}"
    "--s3_eol_surveyresults_destination"                        = "s3://${var.domain_bucket}/Data/SurveyResults"
    "--s3_eol_surveyresults_notlinktoaccountant_destination"    = "s3://${var.domain_bucket}/Data/SurveyResults_NotLinkToAccountant"
    "--raw_eol_units_table"                                     = "${var.raw_eol_units_table}"
    "--s3_eol_units_destination"                                = "s3://${var.domain_bucket}/Data/Units"
    "--raw_eol_usagetransactions_table"                         = "${var.raw_eol_usagetransactions_table}"
    "--s3_eol_usagetransactions_destination"                    = "s3://${var.domain_bucket}/Data/UsageTransactions"
    "--raw_eol_usertypes_table"                                 = "${var.raw_eol_usertypes_table}"
    "--s3_eol_usertypes_destination"                            = "s3://${var.domain_bucket}/Data/UserTypes"
    "--raw_eol_userusertypes_table"                             = "${var.raw_eol_userusertypes_table}"
    "--s3_eol_userusertypes_destination"                        = "s3://${var.domain_bucket}/Data/UserUserTypes"
    "--raw_eol_projects_table"                                  = "${var.raw_eol_projects_table}"
    "--s3_eol_projects_destination"                             = "s3://${var.domain_bucket}/Data/Projects"
    "--raw_eol_timecosttransactions_table"                      = "${var.raw_eol_timecosttransactions_table}"
    "--s3_eol_timecosttransactions_destination"                 = "s3://${var.domain_bucket}/Data/TimeCostTransactions"
    "--raw_eol_usageentitlements_table"                         = "${var.raw_eol_usageentitlements_table}"
    "--s3_eol_usageentitlements_destination"                    = "s3://${var.domain_bucket}/Data/UsageEntitlements"
    "--raw_eol_conversionstatus_table"                          = "${var.raw_eol_conversionstatus_table}"
    "--raw_eol_gltransactionsources_table"                      = "${var.raw_eol_gltransactionsources_table}"
    "--raw_eol_oauthclients_table"                              = "${var.raw_eol_oauthclients_table}"
    "--s3_eol_oauthclients_view_destination"                    = "s3://${var.domain_bucket}/Data/oauthclients_view"
    "--s3_eol_oauthclientslog_view_destination"                 = "s3://${var.domain_bucket}/Data/oauthclientslog_view"
    "--s3_eol_conversionstatus_destination"                     = "s3://${var.domain_bucket}/Data/ConversionSource"
    "--raw_eol_mail2eol_table"                                  = "${var.raw_eol_mail2eol_table}"
    "--s3_eol_mail2eol_destination"                             = "s3://${var.domain_bucket}/Data/Mail2Eol"
    "--raw_eol_scanprovideruploadlog_table"                     = "${var.raw_eol_scanprovideruploadlog_table}"
    "--s3_eol_scanprovideruploadlog_destination"                = "s3://${var.domain_bucket}/Data/ScanProviderUploadLog"
    "--raw_eol_scanninglyanthe_table"                           = "${var.raw_eol_scanninglyanthe_table}"
    "--s3_eol_scanninglyanthe_destination"                      = "s3://${var.domain_bucket}/Data/ScanningLyanthe"
    "--domain_contracts_table"                                  = "${var.domain_contracts_table}"
    "--domain_accountscontract_summary_table"                   = "${var.domain_accountscontract_summary_table}"
    "--domain_accounts_table"                                   = "${var.domain_accounts_table}"
    "--domain_packageclassification_table"                      = "${var.domain_packageclassification_table}"
    "--s3_eol_modules_destination"                              = "s3://${var.domain_bucket}/Data/Modules"
    "--s3_eol_divisionstartuptype_destination"                  = "s3://${var.domain_bucket}/Data/DivisionStartupType"
    "--raw_eol_activeuserdivisions_table"                       = "${var.raw_eol_activeuserdivisions_table}"
    "--s3_eol_activeuserdivisions_destination"                  = "s3://${var.domain_bucket}/Data/ActiveUserDivisions"
    "--raw_eol_activeuserroles_table"                           = "${var.raw_eol_activeuserroles_table}"
    "--s3_eol_activeuserroles_destination"                      = "s3://${var.domain_bucket}/Data/ActiveUserRoles"
    "--raw_eol_divisiontypefeaturesets_table"                   = "${var.raw_eol_divisiontypefeaturesets_table}"
    "--s3_eol_divisiontypefeaturesets_destination"              = "s3://${var.domain_bucket}/Data/DivisionTypeFeatureSets"
    "--job-bookmark-option"                                     = "${var.bookmark}"
  }
}

resource "aws_s3_bucket_object" "Requests_configuration" {
  bucket = "${var.glue_artifact_store}"
  key    = "Glue/${var.environment}/Eol-Hosting/requests_config.json"
  source = "scripts/requests_config.json"
  etag   = "${md5(file("scripts/requests_config.json"))}"
}

resource "aws_s3_bucket_object" "Load_Requests" {
  bucket = "${var.glue_artifact_store}"
  key    = "Glue/${var.environment}/Eol-Hosting/domain.Load_Requests.py"
  source = "scripts/Eol-Hosting/domain.Load_Requests.py"
  etag   = "${md5(file("scripts/Eol-Hosting/domain.Load_Requests.py"))}"
}

resource "aws_glue_job" "Load_Requests" {
  name               = "domain.Load_EolHosting_Requests"
  role_arn           = "${data.aws_iam_role.cig_glue_role.arn}"
  allocated_capacity = "${var.dpu}"

  command {
    script_location = "s3://${var.glue_artifact_store}/${aws_s3_bucket_object.Load_Requests.id}"
  }

  default_arguments = {
    "--extra-py-files"                                     = "s3://${var.glue_artifact_store}/${aws_s3_bucket_object.Utils.id}"
    "--raw_db"                                             = "${var.raw_database}"
    "--raw_eol_requests_table"                             = "${var.raw_eol_requests_table}"
    "--s3_artifact_bucket"                                 = "${var.glue_artifact_store}"
    "--configuration_file_key"                             = "${aws_s3_bucket_object.Requests_configuration.id}"
    "--job-bookmark-option"                                = "${var.bookmark}"
  }
}

resource "aws_s3_bucket_object" "Load_Divisions_MasterDataSetup" {
  bucket = "${var.glue_artifact_store}"
  key    = "Glue/${var.environment}/Eol-Hosting/domain.Load_Divisions_MasterDataSetup.py"
  source = "scripts/Eol-Hosting/domain.Load_Divisions_MasterDataSetup.py"
  etag   = "${md5(file("scripts/Eol-Hosting/domain.Load_Divisions_MasterDataSetup.py"))}"
}

resource "aws_glue_job" "Load_Divisions_MasterDataSetup" {
  name               = "domain.Load_EolHosting_Divisions_MasterDataSetup"
  role_arn           = "${data.aws_iam_role.cig_glue_role.arn}"
  allocated_capacity = "${var.dpu}"

  command {
    script_location = "s3://${var.glue_artifact_store}/${aws_s3_bucket_object.Load_Divisions_MasterDataSetup.id}"
  }

  default_arguments = {
    "--extra-py-files"                                   = "s3://${var.glue_artifact_store}/${aws_s3_bucket_object.Utils.id}"
    "--raw_db"                                           = "${var.raw_database}"
    "--domain_db"                                        = "${var.domain_database}"
    "--raw_eol_divisions_table"                          = "${var.raw_eol_divisions_table}"
    "--domain_divisions_table"                           = "${var.domain_divisions_table}"
    "--domain_accountscontract_summary_table"            = "${var.domain_accountscontract_summary_table}"
    "--domain_divisionstatistics_summary_table"          = "${var.domain_divisionstatistics_summary_table}"
    "--domain_activitydaily_table"                       = "${var.domain_activitydaily_table}"
    "--domain_divisions_masterdatasetup_table"           = "${var.domain_divisions_masterdatasetup_table}"
    "--domain_requests_transferrequest_table"            = "${var.domain_requests_transferrequest_table}"
    "--domain_requests_executetransferrequest_table"     = "${var.domain_requests_executetransferrequest_table}"
    "--domain_contacts_table"                            = "${var.domain_contacts_table}"
    "--domain_users_table"                               = "${var.domain_users_table}"
    "--domain_conversionsource_table"                    = "${var.domain_conversionsource_table}"
    "--s3_bucket"                                        = "${var.domain_bucket}"
    "--s3_destination"                                   = "s3://${var.domain_bucket}/Data/Divisions_MasterDataSetup"
    "--job-bookmark-option"                              = "${var.bookmark}"
  }
}

resource "aws_s3_bucket_object" "Load_DivisionsLog" {
  bucket = "${var.glue_artifact_store}"
  key    = "Glue/${var.environment}/Eol-Hosting/domain.Load_DivisionsLog.py"
  source = "scripts/Eol-Hosting/domain.Load_DivisionsLog.py"
  etag   = "${md5(file("scripts/Eol-Hosting/domain.Load_DivisionsLog.py"))}"
}

resource "aws_glue_job" "Load_DivisionsLog" {
  name               = "domain.Load_DivisionsLog"
  role_arn           = "${data.aws_iam_role.cig_glue_role.arn}"
  allocated_capacity = "5"

  command {
    script_location = "s3://${var.glue_artifact_store}/${aws_s3_bucket_object.Load_DivisionsLog.id}"
  }

  default_arguments = {
    "--raw_db"                      = "${var.raw_database}"
    "--raw_eol_divisions_table"     = "${var.raw_eol_divisions_table}"
    "--s3_destination_division_log" = "s3://${var.domain_bucket}/Data/DivisionsLog"
    "--extra-py-files"              = "s3://${var.glue_artifact_store}/${aws_s3_bucket_object.Utils.id}"
    "--job-bookmark-option" = "${var.bookmark}"
  }
}

resource "aws_glue_trigger" "eol_hosting_trigger" {
  name     = "trigger_eol_hosting_0600_utc"
  type     = "SCHEDULED"
  schedule = "cron(0 6 ? * * *)"
  enabled  = "${var.environment == "prod" ? true : false }"

  actions {
    job_name = "${aws_glue_job.Copy_Data_To_Raw_bucket_Glue_Job.name}"
  }

  actions {
    job_name = "${aws_glue_job.Load_DivisionStatistics_Summary.name}"
  }

  actions {
    job_name = "${aws_glue_job.Load_DivisionStatistics_Daily.name}"
  }

  actions {
    job_name = "${aws_glue_job.DomainTransformations.name}"
  }

  actions {
    job_name = "${aws_glue_job.RawTransformations.name}"
  }

  actions {
    job_name = "${aws_glue_job.Load_Requests.name}"
  }

  actions {
    job_name = "${aws_glue_job.Load_LinkedAccountantLog_New.name}"
  }
}

resource "aws_glue_trigger" "eol_hosting_divisionstatistics_conditional_trigger" {
  name    = "eol_hosting_divisionstatistics_conditional"
  type    = "CONDITIONAL"
  enabled = "${var.environment == "prod" ? true : false }"

  predicate {
    conditions {
      job_name         = "${aws_glue_job.Load_DivisionStatistics_Daily.name}"
      state            = "SUCCEEDED"
      logical_operator = "EQUALS"
    }
  }

  actions {
    job_name = "${aws_glue_job.Load_DivisionStatistics_DailyChanges.name}"
  }

  actions {
    job_name = "${aws_glue_job.Load_DivisionsLog.name}"
  }
}

resource "aws_glue_trigger" "eol_hosting_conditional_trigger" {
  name    = "eol_hosting_conditional"
  type    = "CONDITIONAL"
  enabled = "${var.environment == "prod" ? true : false }"

  predicate {
    conditions {
      job_name         = "${aws_glue_job.Load_DivisionStatistics_Daily.name}"
      state            = "SUCCEEDED"
      logical_operator = "EQUALS"
    }

    conditions {
      job_name         = "${aws_glue_job.Load_DivisionStatistics_Summary.name}"
      state            = "SUCCEEDED"
      logical_operator = "EQUALS"
    }

    conditions {
      job_name         = "${aws_glue_job.DomainTransformations.name}"
      state            = "SUCCEEDED"
      logical_operator = "EQUALS"
    }

    conditions {
      job_name         = "${aws_glue_job.Load_Requests.name}"
      state            = "SUCCEEDED"
      logical_operator = "EQUALS"
    }
  }

  actions {
    job_name = "${aws_glue_job.Load_Divisions_MasterDataSetup.name}"
  }

  actions {
    job_name = "${aws_glue_job.KeyEventsTransformations.name}"
  }
}

resource "aws_dynamodb_table" "lastupdated_table" {
  name           = "${var.lastupdated_table}"
  billing_mode   = "PROVISIONED"
  read_capacity  = 1
  write_capacity = 1
  hash_key       = "Item"

  attribute {
    name = "Item"
    type = "S"
  }
}

resource "aws_s3_bucket_object" "cumulus_copy_configuration" {
  bucket = "${var.glue_artifact_store}"
  key    = "Glue/${var.environment}/Eol-Hosting/cumulus_copy_config.json"
  source = "scripts/cumulus_copy_config.json"
  etag   = "${md5(file("scripts/cumulus_copy_config.json"))}"
}

resource "aws_s3_bucket_object" "Copy_Data_To_Raw_Bucket" {
  bucket = "${var.glue_artifact_store}"
  key    = "Glue/${var.environment}/Eol-Hosting/config.Copy_Cumulus_Data_To_Raw_Bucket.py"
  source = "scripts/Eol-Hosting/config.Copy_Cumulus_Data_To_Raw_Bucket.py"
  etag   = "${md5(file("scripts/Eol-Hosting/config.Copy_Cumulus_Data_To_Raw_Bucket.py"))}"
}

resource "aws_glue_job" "Copy_Data_To_Raw_bucket_Glue_Job" {
  name               = "Config.Copy_Cumulus_Data_To_Raw_Bucket"
  role_arn           = "${data.aws_iam_role.cig_glue_role.arn}"
  allocated_capacity = "${var.dpu}"

  command {
    script_location = "s3://${var.glue_artifact_store}/${aws_s3_bucket_object.Copy_Data_To_Raw_Bucket.id}"
  }

  default_arguments = {
    "--configuration_file_key"                             = "${aws_s3_bucket_object.cumulus_copy_configuration.id}"
    "--source_bucket"                                      = "${var.cumulus_bucket_name}"
    "--destination_bucket"                                 = "${var.raw_bucket_name}"
    "--s3_artifact_bucket"                                 = "${var.glue_artifact_store}"
    "--job-bookmark-option"                                = "${var.bookmark}"
  }
}

resource "aws_s3_bucket_object" "Load_LinkedAccountantLog_New" {
  bucket = "${var.glue_artifact_store}"
  key    = "Glue/${var.environment}/Eol-Hosting/domain.Load_LinkedAccountantLog_New.py"
  source = "scripts/Eol-Hosting/domain.Load_LinkedAccountantLog_New.py"
  etag   = "${md5(file("scripts/Eol-Hosting/domain.Load_LinkedAccountantLog_New.py"))}"
}

resource "aws_glue_job" "Load_LinkedAccountantLog_New" {
  name               = "domain.Load_LinkedAccountantLog_New"
  role_arn           = "${data.aws_iam_role.cig_glue_role.arn}"
  allocated_capacity = "${var.dpu}"

  command {
    script_location = "s3://${var.glue_artifact_store}/${aws_s3_bucket_object.Load_LinkedAccountantLog_New.id}"
  }

  default_arguments = {
    "--extra-py-files"                         = "s3://${var.glue_artifact_store}/${aws_s3_bucket_object.Utils.id}"
    "--raw_db"                                 = "${var.raw_database}"
    "--raw_eol_accounts_table"                 = "${var.raw_eol_accounts_table}"
    "--domain_db"                              = "${var.domain_database}"
    "--config_calendar"                        = "${var.config_calendar}"
    "--s3_destination_linkedaccountantlog_new" = "s3://${var.domain_bucket}/Data/LinkedAccountantLog_New"
  }
}