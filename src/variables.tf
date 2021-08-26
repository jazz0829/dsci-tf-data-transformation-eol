variable "region" {
  default = "eu-west-1"
}

variable "tags_team" {
  default = "cig"
}

variable "app_name" {
  default = "cig-tf-data-transformation-eol"
}

variable "environment" {}

variable "project_name" {
  default = "data-transformation-eol"
}

variable "glue_artifact_store" {
  default = "cig-build-artifact-store"
}

variable "bookmark" {
  default = "job-bookmark-disable"
}

variable "dpu" {
  default = 10
}

variable "domain_bucket" {}

variable "raw_bucket" {}

variable "athena_output_bucket" {}

variable "raw_database" {
  default = "customerintelligence_raw"
}

variable "raw_eol_divisionstatistics_table" {
  default = "object_host_cig_divisionstatistics"
}

variable "raw_eol_divisions_table" {
  default = "object_host_cig_divisions"
}

variable "raw_eol_items_table" {
  default = "object_host_cig_items"
}

variable "raw_eol_contacts_table" {
  default = "object_host_cig_contacts"
}

variable "raw_eol_activitysectors_table" {
  default = "object_host_cig_activitysectors"
}

variable "raw_eol_companysizes_table" {
  default = "object_host_cig_companysizes"
}
variable "raw_eol_users_table" {
  default = "object_host_cig_users"
}

variable "raw_eol_persons_table" {
  default = "object_host_cig_persons"
}

variable "lastupdated_table" {
  default = "Transformations_LastUpdated"
}


variable "raw_eol_projects_table" {
  default = "object_host_cig_projects"
}

variable "raw_eol_timecosttransactions_table" {
  default = "object_host_cig_timecosttransactions"
}

variable "raw_eol_contracts_table" {
  default = "object_host_cig_contracts"
}

variable "raw_eol_usageentitlements_table" {
  default = "object_host_cig_usageentitlements"
}

variable "raw_eol_divisiondivisiontypes_table" {
  default = "object_host_cig_divisiondivisiontypes"
}

variable "raw_eol_divisiontypes_table" {
  default = "object_host_cig_divisiontypes"
}

variable "raw_eol_mail2eol_table" {
  default = "object_mail2eolawslog"
}

variable "raw_eol_scanninglyanthe_table" {
  default = "object_scanninglyanthe"
}

variable "raw_eol_scanprovideruploadlog_table" {
  default = "object_scanprovideruploadlog"
}

variable "raw_eol_creditmanagementstatus_table" {
  default = "object_host_cig_creditmanagementstatus"
}

variable "raw_eol_customersubscriptionstatistics_table" {
  default = "object_host_cig_customersubscriptionstatistics"
}

variable "raw_eol_itemclasses_table" {
  default = "object_host_cig_itemclasses"
}

variable "raw_eol_itemrelations_table" {
  default = "object_host_cig_itemrelations"
}

variable "raw_eol_leadsources_table" {
  default = "object_host_cig_leadsources"
}

variable "raw_eol_opportunities_hosting_table" {
  default = "object_host_cig_opportunities"
}

variable "raw_eol_opportunitystages_table" {
  default = "object_host_cig_opportunitystages"
}

variable "raw_eol_requests_table" {
  default = "object_host_cig_requests"
}

variable "raw_eol_appusagelines_table" {
  default = "object_host_cig_appusagelines"
}

variable "raw_eol_blockingstatus_table" {
  default = "object_host_cig_blockingstatus"
}

variable "raw_eol_businesstypes_table" {
  default = "object_host_cig_businesstypes"
}

variable "raw_eol_contractmutations_table" {
  default = "object_host_cig_contractmutations"
}

variable "raw_eol_contractevents_table" {
  default = "object_host_cig_contractevents"
}

variable "raw_eol_accountsclassifications_table" {
  default = "object_host_cig_classifications"
}

variable "raw_eol_subscriptionquotations_table" {
  default = "object_host_cig_subscriptionquotations"
}

variable "raw_eol_contractlines_table" {
  default = "object_host_cig_contractlines"
}

variable "raw_eol_surveyresults_table" {
  default = "object_host_cig_surveyresults"
}

variable "raw_eol_oauthclients_table" {
  default = "object_host_cig_oauthclients"
}

variable "raw_eol_usertypes_table" {
  default = "object_host_cig_usertypes"
}

variable "raw_eol_accounts_table" {
  default = "object_host_cig_accounts"
}

variable "raw_eol_paymentterms_table" {
  default = "object_host_cig_paymentterms"
}

variable "raw_eol_userusertypes_table" {
  default = "object_host_cig_userusertypes"
}

variable "raw_eol_gltransactionsources_table" {
  default = "object_host_cig_gltransactionsources"
}

variable "raw_eol_documents_table" {
  default = "object_host_cig_documents"
}

variable "raw_eol_units_table" {
  default = "object_host_cig_units"
}

variable "raw_eol_conversionstatus_table" {
  default = "object_host_cig_conversionstatus"
}

variable "raw_eol_usagetransactions_table" {
  default = "object_host_cig_usagetransactions"
}

variable "raw_eol_activeuserdivisions_table" {
  default = "object_host_cig_activeuserdivisions"
}

variable "raw_eol_activeuserroles_table" {
  default = "object_host_cig_activeuserroles"
}

variable "raw_eol_divisiontypefeaturesets_table" {
  default = "object_host_cig_divisiontypefeaturesets"
}

variable "domain_database" {
  default = "customerintelligence"
}

variable "domain_divisionstatistics_daily_table" {
  default = "divisionstatistics_daily"
}

variable "domain_conversionsource_table" {
  default = "conversionsource"
}

variable "domain_divisions_table" {
  default = "divisions"
}

variable "domain_accountscontract_summary_table" {
  default = "accountscontract_summary"
}

variable "domain_divisionstatistics_summary_table" {
  default = "divisionstatistics_summary"
}

variable "domain_requests_consultaanvraag_table" {
  default = "requests_consultaanvraag"
}

variable "domain_activitydaily_table" {
  default = "activitydaily"
}

variable "domain_divisions_masterdatasetup_table" {
  default = "divisions_masterdatasetup"
}

variable "domain_requests_transferrequest_table" {
  default = "requests_transferrequest"
}

variable "domain_requests_executetransferrequest_table" {
  default = "requests_executetransferrequest"
}

variable "domain_contacts_table" {
  default = "contacts"
}

variable "domain_timecosttransactions_table" {
  default = "timecosttransactions"
}

variable "domain_users_table" {
  default = "users"
}

variable "domain_items_table" {
  default = "items"
}

variable "domain_contracts_table" {
  default = "contracts"
}

variable "domain_accounts_table" {
  default = "accounts"
}

variable "domain_keyevents_all_table" {
  default = "keyevents_all"
}

variable "domain_linkedaccountantlog_table" {
  default = "linkedaccountantlog"
}

variable "domain_packageclassification_table" {
  default = "packageclassification"
}

variable "config_calendar" {
  default = "config_calendar"
}

variable "raw_bucket_name" {
  default = "cig-prod-raw-bucket"
}

variable "cumulus_bucket_name" {
  default = "dsci-cig-data-131239767718-eu-west-1"
}