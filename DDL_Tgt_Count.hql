set mapreduce.job.queuename = ingestion;

DROP TABLE IF EXISTS dev_audit.bda_tables_sumrz_conf;

CREATE EXTERNAL TABLE dev_audit.bda_tables_sumrz_conf 
(
   schema_name varchar(40),
   table_id int,
   table_name varchar(100),
   table_type varchar(20),
   count_ind char(1),
   summarized_date string,
   dates_ago int,
   primary_key string,
   sql string
   
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0007'
STORED AS TEXTFILE
LOCATION 
  'hdfs://bda6clu-ns/lake/audit/ecomm/bda_tables_sumrz_conf';
  

msck repair table dev_audit.bda_tables_sumrz_conf;
grant all on audit.bda_tables_sumrz_conf to role table_admin_role;
grant select on audit.bda_tables_sumrz_conf to role pdw_role;
  
INSERT OVERWRITE table dev_audit.bda_tables_sumrz_conf 
select * from ( 
SELECT 'AUDIT' schema_name, 1 table_id, 'FE_PAYMENTS' table_name, 'TRAN' table_type, 'Y' count_ind, 'CREATED_DATE' summarized_date, 10 dates_ago, 'PAYMENT_ID' primary_key, 'SELECT COUNT(DISTINCT PAYMENT_ID ) , TO_DATE(CREATED_DATE) FROM AUDIT.FE_PAYMENTS WHERE to_date(created_date) = {var1} and extract_date between {var1} and date_add({var1},3) group by to_date(created_date)' sql
UNION ALL
SELECT 'AUDIT' schema_name, 2 table_id, 'VAP_RESPONSE_REASON_CODE_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'RESPONSE_REASON_CODE' primary_key, 'SELECT COUNT(DISTINCT RESPONSE_REASON_CODE) , {var1} FROM AUDIT.vap_response_reason_code_ref' sql
) a


DROP TABLE IF EXISTS dev_audit.bda_tables_sumrz_data;

CREATE EXTERNAL TABLE dev_audit.bda_tables_sumrz_data 
(
   table_id int,
   table_name varchar(100),
   summarized_date string,
   runtime_sql string,
   application_id string,
   count bigint,
   start_time string,
   end_time string,
   log string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0007'
STORED AS TEXTFILE
LOCATION 
  'hdfs://bda6clu-ns/lake/audit/ecomm/bda_tables_sumrz_data';
  

msck repair table dev_audit.bda_tables_sumrz_data;

grant all on audit.bda_tables_sumrz_data to role table_admin_role;
grant select on audit.bda_tables_sumrz_data to role pdw_role;
--------------------------------------------------

INSERT OVERWRITE table dev_audit.bda_tables_sumrz_conf 
select * from ( 
SELECT 'AUDIT' schema_name, 1 table_id, 'FE_PAYMENTS' table_name, 'TRAN' table_type, 'Y' count_ind, 'CREATED_DATE' summarized_date, 10 dates_ago, 'PAYMENT_ID' primary_key, 'SELECT COUNT(DISTINCT PAYMENT_ID ) , TO_DATE(CREATED_DATE) FROM AUDIT.FE_PAYMENTS WHERE to_date(created_date) = {var1} and extract_date between {var1} and date_add({var1},3) group by to_date(created_date)' sql
UNION ALL
SELECT 'AUDIT' schema_name, 2 table_id, 'VAP_RESPONSE_REASON_CODE_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'RESPONSE_REASON_CODE' primary_key, 'SELECT COUNT(DISTINCT RESPONSE_REASON_CODE) , {var1} FROM AUDIT.vap_response_reason_code_ref' sql
UNION ALL
SELECT 'AUDIT' schema_name, 3 table_id, 'VAP_MERCHANTS' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'MERCHANT_ID' primary_key, 'SELECT COUNT(DISTINCT MERCHANT_ID) , {var1} FROM AUDIT.VAP_MERCHANTS' sql
UNION ALL
SELECT 'AUDIT' schema_name, 4 table_id, 'VAP_ORGANIZATIONS' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'ORGANIZATION_ID' primary_key, 'SELECT COUNT(DISTINCT ORGANIZATION_ID) , {var1} FROM AUDIT.VAP_ORGANIZATIONS' sql
UNION ALL
SELECT 'AUDIT' schema_name, 5 table_id, 'RPT_TXN_OUTBOUND_SUMMARY' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'SUMMARY_ID' primary_key, 'SELECT COUNT(DISTINCT SUMMARY_ID) , {var1} FROM AUDIT.RPT_TXN_OUTBOUND_SUMMARY' sql
UNION ALL
SELECT 'AUDIT' schema_name, 6 table_id, 'RPT_FINANCIAL_SUMMARY' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'SUMMARY_ID,FUNDS_TRANSFER_UNIT_ID' primary_key, 'SELECT COUNT(DISTINCT SUMMARY_ID,FUNDS_TRANSFER_UNIT_ID) , {var1} FROM AUDIT.RPT_FINANCIAL_SUMMARY' sql
UNION ALL
SELECT 'AUDIT' schema_name, 7 table_id, 'RPT_CATEGORY_NODES' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'NODE_ID' primary_key, 'SELECT COUNT(DISTINCT NODE_ID) , {var1} FROM AUDIT.RPT_CATEGORY_NODES' sql
UNION ALL
SELECT 'AUDIT' schema_name, 8 table_id, 'META_MERCHANT_PROCESSING_GROUPS' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'PROCESSING_GROUP_ID' primary_key, 'SELECT COUNT(DISTINCT PROCESSING_GROUP_ID) , {var1} FROM AUDIT.META_MERCHANT_PROCESSING_GROUPS' sql
UNION ALL
SELECT 'AUDIT' schema_name, 9 table_id, 'VAP_CURRENCY_CODE_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'CURRENCY_CODE' primary_key, 'SELECT COUNT(DISTINCT CURRENCY_CODE) , {var1} FROM AUDIT.VAP_CURRENCY_CODE_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 10 table_id, 'VAP_MCC_KNOWN' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'MERCHANT_CATEGORY_CODE' primary_key, 'SELECT COUNT(DISTINCT MERCHANT_CATEGORY_CODE) , {var1} FROM AUDIT.VAP_MCC_KNOWN' sql
UNION ALL
SELECT 'AUDIT' schema_name, 11 table_id, 'RPT_FEE_SUMMARY' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'SUMMARY_ID' primary_key, 'SELECT COUNT(DISTINCT SUMMARY_ID) , {var1} FROM AUDIT.RPT_FEE_SUMMARY' sql
UNION ALL
SELECT 'AUDIT' schema_name, 12 table_id, 'ACT_FEE_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'FEE_REF_ID' primary_key, 'SELECT COUNT(DISTINCT FEE_REF_ID) , {var1} FROM AUDIT.ACT_FEE_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 13 table_id, 'ACT_FEE_CATEGORY_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'FEE_CATEGORY_ID' primary_key, 'SELECT COUNT(DISTINCT FEE_CATEGORY_ID) , {var1} FROM AUDIT.ACT_FEE_CATEGORY_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 14 table_id, 'VAP_INTERCHANGE_FEE_XREF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'LOOKUP_KEY' primary_key, 'SELECT COUNT(DISTINCT LOOKUP_KEY) , {var1} FROM AUDIT.VAP_INTERCHANGE_FEE_XREF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 15 table_id, 'BILL_PASSTHROUGH_FEE_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'PASSTHROUGH_FEE_ID' primary_key, 'SELECT COUNT(DISTINCT PASSTHROUGH_FEE_ID) , {var1} FROM AUDIT.BILL_PASSTHROUGH_FEE_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 16 table_id, 'VAP_METHOD_OF_PAYMENT_CODE_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'METHOD_OF_PAYMENT_CODE' primary_key, 'SELECT COUNT(DISTINCT METHOD_OF_PAYMENT_CODE) , {var1} FROM AUDIT.VAP_METHOD_OF_PAYMENT_CODE_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 17 table_id, 'VAP_PAYMENT_PROCESSORS' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'PAYMENT_PROCESSOR_ID' primary_key, 'SELECT COUNT(DISTINCT PAYMENT_PROCESSOR_ID) , {var1} FROM AUDIT.VAP_PAYMENT_PROCESSORS' sql
UNION ALL
SELECT 'AUDIT' schema_name, 18 table_id, 'VAP_TRANSACTION_TYPE_CODE_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'TRANSACTION_TYPE_CODE' primary_key, 'SELECT COUNT(DISTINCT TRANSACTION_TYPE_CODE) , {var1} FROM AUDIT.VAP_TRANSACTION_TYPE_CODE_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 19 table_id, 'VAP_VANTIV_AUTH_RESPONSE_REASON_CODE_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'VANTIV_AUTH_RRC_ID' primary_key, 'SELECT COUNT(DISTINCT VANTIV_AUTH_RRC_ID) , {var1} FROM AUDIT.VAP_VANTIV_AUTH_RESPONSE_REASON_CODE_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 20 table_id, 'CBK_QUEUE_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'QUEUE_ID' primary_key, 'SELECT COUNT(DISTINCT QUEUE_ID) , {var1} FROM AUDIT.CBK_QUEUE_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 21 table_id, 'VAP_EXCHANGE_RATES' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'EXCHANGE_RATE_ID' primary_key, 'SELECT COUNT(DISTINCT EXCHANGE_RATE_ID) , {var1} FROM AUDIT.VAP_EXCHANGE_RATES' sql
UNION ALL
SELECT 'AUDIT' schema_name, 22 table_id, 'ACT_FEES' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'FEE_ID' primary_key, 'SELECT COUNT(DISTINCT FEE_ID) , {var1} FROM AUDIT.ACT_FEES' sql
UNION ALL
SELECT 'AUDIT' schema_name, 23 table_id, 'MP_LEGAL_ENTITY' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'LEGAL_ENTITY_ID_PK' primary_key, 'SELECT COUNT(DISTINCT LEGAL_ENTITY_ID_PK) , {var1} FROM AUDIT.MP_LEGAL_ENTITY' sql
UNION ALL
SELECT 'AUDIT' schema_name, 24 table_id, 'MP_SUB_MERCHANT' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'SUB_MERCHANT_ID_PK' primary_key, 'SELECT COUNT(DISTINCT SUB_MERCHANT_ID_PK) , {var1} FROM AUDIT.MP_SUB_MERCHANT' sql
UNION ALL
SELECT 'AUDIT' schema_name, 25 table_id, 'VAP_BINS' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'BIN_ID' primary_key, 'SELECT COUNT(DISTINCT BIN_ID) , {var1} FROM AUDIT.VAP_BINS' sql
UNION ALL
SELECT 'AUDIT' schema_name, 26 table_id, 'ACT_BILLING_TYPE_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'BILLING_TYPE_ID' primary_key, 'SELECT COUNT(DISTINCT BILLING_TYPE_ID) , {var1} FROM AUDIT.ACT_BILLING_TYPE_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 27 table_id, 'ACT_FEE_RPT_GROUP_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'FEE_RPT_GROUP_REF_ID' primary_key, 'SELECT COUNT(DISTINCT FEE_RPT_GROUP_REF_ID) , {var1} FROM AUDIT.ACT_FEE_RPT_GROUP_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 28 table_id, 'ACT_LEDGER_POSTINGS_TO_FEES_XREF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'LEDGER_POSTING_ID' primary_key, 'SELECT COUNT(DISTINCT LEDGER_POSTING_ID) , {var1} FROM AUDIT.ACT_LEDGER_POSTINGS_TO_FEES_XREF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 29 table_id, 'VAP_VISA_FRAUD_ADVICE' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'ID' primary_key, 'SELECT COUNT(DISTINCT ID) , {var1} FROM AUDIT.VAP_VISA_FRAUD_ADVICE' sql
UNION ALL
SELECT 'AUDIT' schema_name, 30 table_id, 'VAP_VISA_PROD_ID_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'PROD_ID_CODE' primary_key, 'SELECT COUNT(DISTINCT PROD_ID_CODE) , {var1} FROM AUDIT.VAP_VISA_PROD_ID_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 31 table_id, 'VAP_WFGROUP_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'WFGROUP_ID' primary_key, 'SELECT COUNT(DISTINCT WFGROUP_ID) , {var1} FROM AUDIT.VAP_WFGROUP_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 32 table_id, 'VAP_ACQUIRING_BANK' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'BANK_ID' primary_key, 'SELECT COUNT(DISTINCT BANK_ID) , {var1} FROM AUDIT.VAP_ACQUIRING_BANK' sql
UNION ALL
SELECT 'AUDIT' schema_name, 33 table_id, 'VAP_ACQUIRING_BANK_PROFILE' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'ACQUIRING_BANK_PROFILE_ID' primary_key, 'SELECT COUNT(DISTINCT ACQUIRING_BANK_PROFILE_ID) , {var1} FROM AUDIT.VAP_ACQUIRING_BANK_PROFILE' sql
UNION ALL
SELECT 'AUDIT' schema_name, 34 table_id, 'VAP_MERCHANT_BILLING_PROFILE' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'ID' primary_key, 'SELECT COUNT(DISTINCT ID) , {var1} FROM AUDIT.VAP_MERCHANT_BILLING_PROFILE' sql
UNION ALL
SELECT 'AUDIT' schema_name, 35 table_id, 'VAP_MERCHANT_PROCESSING_GROUP' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'ID' primary_key, 'SELECT COUNT(DISTINCT ID) , {var1} FROM AUDIT.VAP_MERCHANT_PROCESSING_GROUP' sql
UNION ALL
SELECT 'AUDIT' schema_name, 36 table_id, 'VAP_PRESENTER_MERCHANT_XREF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'PRESENTER_ID,MERCHANT_IDENT_STRING' primary_key, 'SELECT COUNT(DISTINCT PRESENTER_ID,MERCHANT_IDENT_STRING) , {var1} FROM AUDIT.VAP_PRESENTER_MERCHANT_XREF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 37 table_id, 'VAP_PRESENTERS' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'PRESENTER_ID' primary_key, 'SELECT COUNT(DISTINCT PRESENTER_ID) , {var1} FROM AUDIT.VAP_PRESENTERS' sql
UNION ALL
SELECT 'AUDIT' schema_name, 38 table_id, 'ACT_FEE_PROFILE_DRIVEN' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'FEE_ID' primary_key, 'SELECT COUNT(DISTINCT FEE_ID) , {var1} FROM AUDIT.ACT_FEE_PROFILE_DRIVEN' sql
UNION ALL
SELECT 'AUDIT' schema_name, 39 table_id, 'ACT_FEE_TYPE_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'FEE_TYPE_REF_ID' primary_key, 'SELECT COUNT(DISTINCT FEE_TYPE_REF_ID) , {var1} FROM AUDIT.ACT_FEE_TYPE_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 40 table_id, 'BILL_BILLING_PROFILE' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'BILLING_PROFILE_ID' primary_key, 'SELECT COUNT(DISTINCT BILLING_PROFILE_ID) , {var1} FROM AUDIT.BILL_BILLING_PROFILE' sql
UNION ALL
SELECT 'AUDIT' schema_name, 41 table_id, 'VAP_MERCHANT_NETWORK_PROFILE' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'ID' primary_key, 'SELECT COUNT(DISTINCT ID) , {var1} FROM AUDIT.VAP_MERCHANT_NETWORK_PROFILE' sql
UNION ALL
SELECT 'AUDIT' schema_name, 42 table_id, 'VAP_MERCHANT_SERVICES' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'MERCHANT_ID' primary_key, 'SELECT COUNT(DISTINCT MERCHANT_ID) , {var1} FROM AUDIT.VAP_MERCHANT_SERVICES' sql
UNION ALL
SELECT 'AUDIT' schema_name, 43 table_id, 'VAP_SETTLEMENT_ROUTING_PROFILE' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'SETTLEMENT_ROUTING_PROFILE_ID,METHOD_OF_PAYMENT_CODE,PAYMENT_PROCESSOR_ID' primary_key, 'SELECT COUNT(DISTINCT SETTLEMENT_ROUTING_PROFILE_ID,METHOD_OF_PAYMENT_CODE,PAYMENT_PROCESSOR_ID) , {var1} FROM AUDIT.VAP_SETTLEMENT_ROUTING_PROFILE' sql
UNION ALL
SELECT 'AUDIT' schema_name, 44 table_id, 'ACT_BILLING_FREQUENCY' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'BILLING_FREQUENCY_ID' primary_key, 'SELECT COUNT(DISTINCT BILLING_FREQUENCY_ID) , {var1} FROM AUDIT.ACT_BILLING_FREQUENCY' sql
UNION ALL
SELECT 'AUDIT' schema_name, 45 table_id, 'ACT_FEE_SCHEDULE' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'FEE_SCHEDULE_ID' primary_key, 'SELECT COUNT(DISTINCT FEE_SCHEDULE_ID) , {var1} FROM AUDIT.ACT_FEE_SCHEDULE' sql
UNION ALL
SELECT 'AUDIT' schema_name, 46 table_id, 'ACT_FEE_SCHEDULE_TIER_BY_DAY' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'FEE_SCHEDULE_ID,DAY,PAYLOAD' primary_key, 'SELECT COUNT(DISTINCT FEE_SCHEDULE_ID,DAY,PAYLOAD) , {var1} FROM AUDIT.ACT_FEE_SCHEDULE_TIER_BY_DAY' sql
UNION ALL
SELECT 'AUDIT' schema_name, 47 table_id, 'ACT_FEE_TIER' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'ID' primary_key, 'SELECT COUNT(DISTINCT ID) , {var1} FROM AUDIT.ACT_FEE_TIER' sql
UNION ALL
SELECT 'AUDIT' schema_name, 48 table_id, 'ACT_FEE_TIER_BASIS' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'ID' primary_key, 'SELECT COUNT(DISTINCT ID) , {var1} FROM AUDIT.ACT_FEE_TIER_BASIS' sql
UNION ALL
SELECT 'AUDIT' schema_name, 49 table_id, 'RPT_MC_MERCHANT_LOCATION_SUMMARY' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'SUMMARY_ID' primary_key, 'SELECT COUNT(DISTINCT SUMMARY_ID) , {var1} FROM AUDIT.RPT_MC_MERCHANT_LOCATION_SUMMARY' sql
UNION ALL
SELECT 'AUDIT' schema_name, 50 table_id, 'RPT_VISA_FANF_SUMMARY' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'SUMMARY_ID' primary_key, 'SELECT COUNT(DISTINCT SUMMARY_ID) , {var1} FROM AUDIT.RPT_VISA_FANF_SUMMARY' sql
UNION ALL
SELECT 'AUDIT' schema_name, 51 table_id, 'USR_USER_PERMISSION_XREF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'USER_ID,PERMISSION' primary_key, 'SELECT COUNT(DISTINCT USER_ID,PERMISSION) , {var1} FROM AUDIT.USR_USER_PERMISSION_XREF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 52 table_id, 'ACT_BILLING_FREQUENCY_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'BILLING_FREQUENCY_REF_ID' primary_key, 'SELECT COUNT(DISTINCT BILLING_FREQUENCY_REF_ID) , {var1} FROM AUDIT.ACT_BILLING_FREQUENCY_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 53 table_id, 'ACT_FEE_EXCLUSIONS' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'FEE_REF_ID,BILLING_PROFILE_ID' primary_key, 'SELECT COUNT(DISTINCT FEE_REF_ID,BILLING_PROFILE_ID) , {var1} FROM AUDIT.ACT_FEE_EXCLUSIONS' sql
UNION ALL
SELECT 'AUDIT' schema_name, 54 table_id, 'ACT_FEE_REF_TO_VANTIV_FEE_XREF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'FEE_REF_ID' primary_key, 'SELECT COUNT(DISTINCT FEE_REF_ID) , {var1} FROM AUDIT.ACT_FEE_REF_TO_VANTIV_FEE_XREF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 55 table_id, 'ACT_FEE_ROUNDING_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'FEE_ROUNDING_CODE' primary_key, 'SELECT COUNT(DISTINCT FEE_ROUNDING_CODE) , {var1} FROM AUDIT.ACT_FEE_ROUNDING_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 56 table_id, 'ACT_FEE_TRANSFER_TYPE_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'FEE_TRANSFER_TYPE_REF_ID' primary_key, 'SELECT COUNT(DISTINCT FEE_TRANSFER_TYPE_REF_ID) , {var1} FROM AUDIT.ACT_FEE_TRANSFER_TYPE_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 57 table_id, 'BILL_BILLING_PROFILE_FEE_SCHEDULE' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'ID' primary_key, 'SELECT COUNT(DISTINCT ID) , {var1} FROM AUDIT.BILL_BILLING_PROFILE_FEE_SCHEDULE' sql
UNION ALL
SELECT 'AUDIT' schema_name, 58 table_id, 'RPT_MC_MERCHANT_LOCATION_BILLING_CRITERIA_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'CRITERIA_ID' primary_key, 'SELECT COUNT(DISTINCT CRITERIA_ID) , {var1} FROM AUDIT.RPT_MC_MERCHANT_LOCATION_BILLING_CRITERIA_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 59 table_id, 'ACCT_ACCOUNTS_TO_BILLING_PROFILE_XREF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'BILLING_PROFILE_ID,ACCOUNT_ID' primary_key, 'SELECT COUNT(DISTINCT BILLING_PROFILE_ID,ACCOUNT_ID) , {var1} FROM AUDIT.ACCT_ACCOUNTS_TO_BILLING_PROFILE_XREF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 60 table_id, 'USR_MOU_USER_ORG_GROUP_XREF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'USER_ORG_ID,GROUP' primary_key, 'SELECT COUNT(DISTINCT USER_ORG_ID,GROUP) , {var1} FROM AUDIT.USR_MOU_USER_ORG_GROUP_XREF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 61 table_id, 'USR_USER_CATEGORY_NODE_XREF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'USER_ID,NODE_ID' primary_key, 'SELECT COUNT(DISTINCT USER_ID,NODE_ID) , {var1} FROM AUDIT.USR_USER_CATEGORY_NODE_XREF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 62 table_id, 'USR_USER_GROUP_XREF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'USER_ID,GROUP' primary_key, 'SELECT COUNT(DISTINCT USER_ID,GROUP) , {var1} FROM AUDIT.USR_USER_GROUP_XREF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 63 table_id, 'VAP_MC_ACCOUNT_RANGES' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'ACCOUNT_RANGE_ID' primary_key, 'SELECT COUNT(DISTINCT ACCOUNT_RANGE_ID) , {var1} FROM AUDIT.VAP_MC_ACCOUNT_RANGES' sql
UNION ALL
SELECT 'AUDIT' schema_name, 64 table_id, 'VAP_MC_PROD_ID_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'PRODUCT_ID_CODE' primary_key, 'SELECT COUNT(DISTINCT PRODUCT_ID_CODE) , {var1} FROM AUDIT.VAP_MC_PROD_ID_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 65 table_id, 'VAP_VISA_ACCOUNT_RANGES' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'ACCOUNT_RANGE_ID' primary_key, 'SELECT COUNT(DISTINCT ACCOUNT_RANGE_ID) , {var1} FROM AUDIT.VAP_VISA_ACCOUNT_RANGES' sql
UNION ALL
SELECT 'AUDIT' schema_name, 66 table_id, 'CBK_CYCLE_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'ID' primary_key, 'SELECT COUNT(DISTINCT ID) , {var1} FROM AUDIT.CBK_CYCLE_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 67 table_id, 'USR_GROUP_PERMISSION_XREF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'GROUP,PERMISSION' primary_key, 'SELECT COUNT(DISTINCT GROUP,PERMISSION) , {var1} FROM AUDIT.USR_GROUP_PERMISSION_XREF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 68 table_id, 'USR_MOU_USER_ORG_PERMISSION_XREF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'USER_ORG_ID,PERMISSION' primary_key, 'SELECT COUNT(DISTINCT USER_ORG_ID,PERMISSION) , {var1} FROM AUDIT.USR_MOU_USER_ORG_PERMISSION_XREF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 69 table_id, 'USR_USERS' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'USER_ID' primary_key, 'SELECT COUNT(DISTINCT USER_ID) , {var1} FROM AUDIT.USR_USERS' sql
UNION ALL
SELECT 'AUDIT' schema_name, 70 table_id, 'VAP_BATCH_TYPE_CODE_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'BATCH_TYPE_CODE' primary_key, 'SELECT COUNT(DISTINCT BATCH_TYPE_CODE) , {var1} FROM AUDIT.VAP_BATCH_TYPE_CODE_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 71 table_id, 'VAP_DI_ACCOUNT_RANGES' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'ACCOUNT_RANGE_ID' primary_key, 'SELECT COUNT(DISTINCT ACCOUNT_RANGE_ID) , {var1} FROM AUDIT.VAP_DI_ACCOUNT_RANGES' sql
UNION ALL
SELECT 'AUDIT' schema_name, 72 table_id, 'VAP_DI_PROD_ID_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'PROD_ID_CODE' primary_key, 'SELECT COUNT(DISTINCT PROD_ID_CODE) , {var1} FROM AUDIT.VAP_DI_PROD_ID_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 73 table_id, 'VAP_ISO_COUNTRY_CODE_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'COUNTRY_CODE_NUMERIC' primary_key, 'SELECT COUNT(DISTINCT COUNTRY_CODE_NUMERIC) , {var1} FROM AUDIT.VAP_ISO_COUNTRY_CODE_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 74 table_id, 'VAP_MERCHANT_BILLING_DESCRIPTOR' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'MERCHANT_ID' primary_key, 'SELECT COUNT(DISTINCT MERCHANT_ID) , {var1} FROM AUDIT.VAP_MERCHANT_BILLING_DESCRIPTOR' sql
UNION ALL
SELECT 'AUDIT' schema_name, 75 table_id, 'VAP_PHOENIXML_RESPONSE_REASON_CODE_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'PHOENIX_RRC_ID' primary_key, 'SELECT COUNT(DISTINCT PHOENIX_RRC_ID) , {var1} FROM AUDIT.VAP_PHOENIXML_RESPONSE_REASON_CODE_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 76 table_id, 'VAP_RECYCLE_CARD_PRODID_XREF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'PATTERN_GROUP_ID,PRODUCT_ID_CODE,METHOD_OF_PAYMENT_CODE' primary_key, 'SELECT COUNT(DISTINCT PATTERN_GROUP_ID,PRODUCT_ID_CODE,METHOD_OF_PAYMENT_CODE) , {var1} FROM AUDIT.VAP_RECYCLE_CARD_PRODID_XREF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 77 table_id, 'VAP_RECYCLE_PATTERNS_XREF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'FULL_PATTERN_ID' primary_key, 'SELECT COUNT(DISTINCT FULL_PATTERN_ID) , {var1} FROM AUDIT.VAP_RECYCLE_PATTERNS_XREF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 78 table_id, 'VAP_RECYCLE_RRC_XREF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'RESPONSE_REASON_CODE,RRC_ADVICE_CATEGORY' primary_key, 'SELECT COUNT(DISTINCT RESPONSE_REASON_CODE,RRC_ADVICE_CATEGORY) , {var1} FROM AUDIT.VAP_RECYCLE_RRC_XREF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 79 table_id, 'VAP_RESPONSE_REASON_CODE_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'RESPONSE_REASON_CODE' primary_key, 'SELECT COUNT(DISTINCT RESPONSE_REASON_CODE) , {var1} FROM AUDIT.VAP_RESPONSE_REASON_CODE_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 80 table_id, 'VAP_TO_FORMAT_RRC_XREF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'RESPONSE_REASON_CODE' primary_key, 'SELECT COUNT(DISTINCT RESPONSE_REASON_CODE) , {var1} FROM AUDIT.VAP_TO_FORMAT_RRC_XREF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 81 table_id, 'USR_LOGIN_STATUS' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'USER_ID' primary_key, 'SELECT COUNT(DISTINCT USER_ID) , {var1} FROM AUDIT.USR_LOGIN_STATUS' sql
UNION ALL
SELECT 'AUDIT' schema_name, 82 table_id, 'USR_USER_ORG_XREF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'USER_ORG_ID' primary_key, 'SELECT COUNT(DISTINCT USER_ORG_ID) , {var1} FROM AUDIT.USR_USER_ORG_XREF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 83 table_id, 'VAP_PAYFAC_CONFIG' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'ID' primary_key, 'SELECT COUNT(DISTINCT ID) , {var1} FROM AUDIT.VAP_PAYFAC_CONFIG' sql
UNION ALL
SELECT 'AUDIT' schema_name, 84 table_id, 'VAP_TIN_FILE' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'TIN_FILE_ID' primary_key, 'SELECT COUNT(DISTINCT TIN_FILE_ID) , {var1} FROM AUDIT.VAP_TIN_FILE' sql
UNION ALL
SELECT 'AUDIT' schema_name, 85 table_id, 'VAP_TIN_FILE_TRACKING' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'TIN_FILE_ID,LEGAL_ENTITY_ID' primary_key, 'SELECT COUNT(DISTINCT TIN_FILE_ID,LEGAL_ENTITY_ID) , {var1} FROM AUDIT.VAP_TIN_FILE_TRACKING' sql
UNION ALL
SELECT 'AUDIT' schema_name, 86 table_id, 'RPT_OPERATING_ACCOUNT_GROUPING_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'RPT_OPERATING_ACCOUNT_GROUPING_REF_ID' primary_key, 'SELECT COUNT(DISTINCT RPT_OPERATING_ACCOUNT_GROUPING_REF_ID) , {var1} FROM AUDIT.RPT_OPERATING_ACCOUNT_GROUPING_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 87 table_id, 'USR_GROUP_TYPE_XREF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'GROUP,ORGANIZATION_TYPE_CODE' primary_key, 'SELECT COUNT(DISTINCT GROUP,ORGANIZATION_TYPE_CODE) , {var1} FROM AUDIT.USR_GROUP_TYPE_XREF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 88 table_id, 'USR_PERMISSION_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'ID' primary_key, 'SELECT COUNT(DISTINCT ID) , {var1} FROM AUDIT.USR_PERMISSION_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 89 table_id, 'VAP_AMEX_LOCATION_REGION_CODE_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'REGION_CODE_ID' primary_key, 'SELECT COUNT(DISTINCT REGION_CODE_ID) , {var1} FROM AUDIT.VAP_AMEX_LOCATION_REGION_CODE_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 90 table_id, 'VAP_BATCH_STATUS_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'BATCH_STATUS' primary_key, 'SELECT COUNT(DISTINCT BATCH_STATUS) , {var1} FROM AUDIT.VAP_BATCH_STATUS_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 91 table_id, 'VAP_MERCHANT_AUTH_FEATURES' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'MERCHANT_ID,AUTH_FEATURE' primary_key, 'SELECT COUNT(DISTINCT MERCHANT_ID,AUTH_FEATURE) , {var1} FROM AUDIT.VAP_MERCHANT_AUTH_FEATURES' sql
UNION ALL
SELECT 'AUDIT' schema_name, 92 table_id, 'VAP_RECYCLE_SEND_TIME_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'BIN,RRC_ADVICE_CATEGORY' primary_key, 'SELECT COUNT(DISTINCT BIN,RRC_ADVICE_CATEGORY) , {var1} FROM AUDIT.VAP_RECYCLE_SEND_TIME_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 93 table_id, 'AU_ACCOUNT_UPDATER_TYPE_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'ACCOUNT_UPDATER_TYPE' primary_key, 'SELECT COUNT(DISTINCT ACCOUNT_UPDATER_TYPE) , {var1} FROM AUDIT.AU_ACCOUNT_UPDATER_TYPE_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 94 table_id, 'CBK_REASON_CODE_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'ID' primary_key, 'SELECT COUNT(DISTINCT ID) , {var1} FROM AUDIT.CBK_REASON_CODE_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 95 table_id, 'CBK_STATUS_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'STATUS_ID' primary_key, 'SELECT COUNT(DISTINCT STATUS_ID) , {var1} FROM AUDIT.CBK_STATUS_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 96 table_id, 'FTP_SESSION_STATUS_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'STATUS' primary_key, 'SELECT COUNT(DISTINCT STATUS) , {var1} FROM AUDIT.FTP_SESSION_STATUS_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 97 table_id, 'RPT_NAME_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'REPORT_NAME_ID' primary_key, 'SELECT COUNT(DISTINCT REPORT_NAME_ID) , {var1} FROM AUDIT.RPT_NAME_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 98 table_id, 'RPT_OPERATING_ACCOUNT_FEE_TYPE_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'ROA_FEE_TYPE_ID' primary_key, 'SELECT COUNT(DISTINCT ROA_FEE_TYPE_ID) , {var1} FROM AUDIT.RPT_OPERATING_ACCOUNT_FEE_TYPE_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 99 table_id, 'RPT_OPERATING_ACCOUNT_SUMMARY' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'RPT_OPERATING_ACCOUNT_SUMMARY_ID' primary_key, 'SELECT COUNT(DISTINCT RPT_OPERATING_ACCOUNT_SUMMARY_ID) , {var1} FROM AUDIT.RPT_OPERATING_ACCOUNT_SUMMARY' sql
UNION ALL
SELECT 'AUDIT' schema_name, 100 table_id, 'RPT_OPERATING_ACCOUNT_TO_CBK_INB_BATCHES_XREF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'RPT_OPERATING_ACCOUNT_ID,INBOUND_BATCH_ID,NETWORK_TXN_ID' primary_key, 'SELECT COUNT(DISTINCT RPT_OPERATING_ACCOUNT_ID,INBOUND_BATCH_ID,NETWORK_TXN_ID) , {var1} FROM AUDIT.RPT_OPERATING_ACCOUNT_TO_CBK_INB_BATCHES_XREF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 101 table_id, 'RPT_OPERATING_ACCOUNT_TO_CBK_OUT_BATCHES_XREF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'RPT_OPERATING_ACCOUNT_ID,OUTBOUND_BATCH_ID,NETWORK_TXN_ID' primary_key, 'SELECT COUNT(DISTINCT RPT_OPERATING_ACCOUNT_ID,OUTBOUND_BATCH_ID,NETWORK_TXN_ID) , {var1} FROM AUDIT.RPT_OPERATING_ACCOUNT_TO_CBK_OUT_BATCHES_XREF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 102 table_id, 'RPT_OPERATING_ACCOUNT_TO_OUTBOUND_XREF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'RPT_OPERATING_ACCOUNT_ID,OUTBOUND_BATCH_ID,PAYMENT_ID' primary_key, 'SELECT COUNT(DISTINCT RPT_OPERATING_ACCOUNT_ID,OUTBOUND_BATCH_ID,PAYMENT_ID) , {var1} FROM AUDIT.RPT_OPERATING_ACCOUNT_TO_OUTBOUND_XREF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 103 table_id, 'USR_GROUP_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'GROUP' primary_key, 'SELECT COUNT(DISTINCT GROUP) , {var1} FROM AUDIT.USR_GROUP_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 104 table_id, 'USR_OLD_PASSWORDS' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'USER_ID,CREATE_DATE' primary_key, 'SELECT COUNT(DISTINCT USER_ID,CREATE_DATE) , {var1} FROM AUDIT.USR_OLD_PASSWORDS' sql
UNION ALL
SELECT 'AUDIT' schema_name, 105 table_id, 'USR_PERMISSION_TYPE_XREF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'PERMISSION,ORGANIZATION_TYPE_CODE' primary_key, 'SELECT COUNT(DISTINCT PERMISSION,ORGANIZATION_TYPE_CODE) , {var1} FROM AUDIT.USR_PERMISSION_TYPE_XREF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 106 table_id, 'VAP_ACCOUNT_UPDATE_BATCH_XREF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'REQUEST_BATCH_ID,RESPONSE_BATCH_ID' primary_key, 'SELECT COUNT(DISTINCT REQUEST_BATCH_ID,RESPONSE_BATCH_ID) , {var1} FROM AUDIT.VAP_ACCOUNT_UPDATE_BATCH_XREF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 107 table_id, 'VAP_ACTION_TYPE_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'ACTION_TYPE' primary_key, 'SELECT COUNT(DISTINCT ACTION_TYPE) , {var1} FROM AUDIT.VAP_ACTION_TYPE_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 108 table_id, 'VAP_ADDRESS_TYPE_CODE_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'ADDRESS_TYPE_CODE' primary_key, 'SELECT COUNT(DISTINCT ADDRESS_TYPE_CODE) , {var1} FROM AUDIT.VAP_ADDRESS_TYPE_CODE_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 109 table_id, 'VAP_AVS_RESULT_CODE_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'AVS_RESULT_CODE' primary_key, 'SELECT COUNT(DISTINCT AVS_RESULT_CODE) , {var1} FROM AUDIT.VAP_AVS_RESULT_CODE_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 110 table_id, 'VAP_BIN_CONTACTS' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'ID' primary_key, 'SELECT COUNT(DISTINCT ID) , {var1} FROM AUDIT.VAP_BIN_CONTACTS' sql
UNION ALL
SELECT 'AUDIT' schema_name, 111 table_id, 'VAP_BIN_TO_CONTACTS_XREF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'BIN_ID,CONTACT_ID' primary_key, 'SELECT COUNT(DISTINCT BIN_ID,CONTACT_ID) , {var1} FROM AUDIT.VAP_BIN_TO_CONTACTS_XREF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 112 table_id, 'VAP_CARD_AU_MAP' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'CARD_AU_MAP_ID' primary_key, 'SELECT COUNT(DISTINCT CARD_AU_MAP_ID) , {var1} FROM AUDIT.VAP_CARD_AU_MAP' sql
UNION ALL
SELECT 'AUDIT' schema_name, 113 table_id, 'VAP_FORMAT_CODE_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'FORMAT_CODE' primary_key, 'SELECT COUNT(DISTINCT FORMAT_CODE) , {var1} FROM AUDIT.VAP_FORMAT_CODE_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 114 table_id, 'VAP_FRAUD_CHECK_RESULT_CODE_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'FRAUD_CHECK_RESULT_CODE' primary_key, 'SELECT COUNT(DISTINCT FRAUD_CHECK_RESULT_CODE) , {var1} FROM AUDIT.VAP_FRAUD_CHECK_RESULT_CODE_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 115 table_id, 'VAP_IBV_BATCH_STATUS_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'IBV_BATCH_STATUS_ID' primary_key, 'SELECT COUNT(DISTINCT IBV_BATCH_STATUS_ID) , {var1} FROM AUDIT.VAP_IBV_BATCH_STATUS_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 116 table_id, 'VAP_INBOUND_OUTBOUND_BATCH_XREF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'INBOUND_BATCH_ID,OUTBOUND_BATCH_ID' primary_key, 'SELECT COUNT(DISTINCT INBOUND_BATCH_ID,OUTBOUND_BATCH_ID) , {var1} FROM AUDIT.VAP_INBOUND_OUTBOUND_BATCH_XREF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 117 table_id, 'VAP_LEAST_COST_ROUTING_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'LCR_ID' primary_key, 'SELECT COUNT(DISTINCT LCR_ID) , {var1} FROM AUDIT.VAP_LEAST_COST_ROUTING_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 118 table_id, 'VAP_MERCHANT_AMMF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'ID' primary_key, 'SELECT COUNT(DISTINCT ID) , {var1} FROM AUDIT.VAP_MERCHANT_AMMF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 119 table_id, 'VAP_MERCHANT_AUTH_FEATURE_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'AUTH_FEATURE' primary_key, 'SELECT COUNT(DISTINCT AUTH_FEATURE) , {var1} FROM AUDIT.VAP_MERCHANT_AUTH_FEATURE_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 120 table_id, 'VAP_MERCHANT_CUSTOM_BILLING_DESCRIPTION_LIST' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'ID' primary_key, 'SELECT COUNT(DISTINCT ID) , {var1} FROM AUDIT.VAP_MERCHANT_CUSTOM_BILLING_DESCRIPTION_LIST' sql
UNION ALL
SELECT 'AUDIT' schema_name, 121 table_id, 'VAP_MERCHANT_DELIVERY_OPTIONS' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'MERCHANT_ID,DELIVERY_OPTION' primary_key, 'SELECT COUNT(DISTINCT MERCHANT_ID,DELIVERY_OPTION) , {var1} FROM AUDIT.VAP_MERCHANT_DELIVERY_OPTIONS' sql
UNION ALL
SELECT 'AUDIT' schema_name, 122 table_id, 'VAP_MERCHANT_PAYMENT_METHODS' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'ID' primary_key, 'SELECT COUNT(DISTINCT ID) , {var1} FROM AUDIT.VAP_MERCHANT_PAYMENT_METHODS' sql
UNION ALL
SELECT 'AUDIT' schema_name, 123 table_id, 'VAP_MERCHANT_PRESENTER_TOKGROUP_XREF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'ID' primary_key, 'SELECT COUNT(DISTINCT ID) , {var1} FROM AUDIT.VAP_MERCHANT_PRESENTER_TOKGROUP_XREF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 124 table_id, 'VAP_MERCHANT_STATUS_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'MERCHANT_STATUS' primary_key, 'SELECT COUNT(DISTINCT MERCHANT_STATUS) , {var1} FROM AUDIT.VAP_MERCHANT_STATUS_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 125 table_id, 'VAP_MERCHANT_TAG' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'TAG_ID' primary_key, 'SELECT COUNT(DISTINCT TAG_ID) , {var1} FROM AUDIT.VAP_MERCHANT_TAG' sql
UNION ALL
SELECT 'AUDIT' schema_name, 126 table_id, 'VAP_MERCHANT_TO_BML_MERCHANT' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'VAP_MERCHANT_TO_BML_ID' primary_key, 'SELECT COUNT(DISTINCT VAP_MERCHANT_TO_BML_ID) , {var1} FROM AUDIT.VAP_MERCHANT_TO_BML_MERCHANT' sql
UNION ALL
SELECT 'AUDIT' schema_name, 127 table_id, 'VAP_MERCHANT_TRANSACTION_PROCESSING' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'ID' primary_key, 'SELECT COUNT(DISTINCT ID) , {var1} FROM AUDIT.VAP_MERCHANT_TRANSACTION_PROCESSING' sql
UNION ALL
SELECT 'AUDIT' schema_name, 128 table_id, 'VAP_MERCHANT_VELOCITY_PROFILES' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'MERCHANT_ID,TXN_TYPE' primary_key, 'SELECT COUNT(DISTINCT MERCHANT_ID,TXN_TYPE) , {var1} FROM AUDIT.VAP_MERCHANT_VELOCITY_PROFILES' sql
UNION ALL
SELECT 'AUDIT' schema_name, 129 table_id, 'VAP_ORGANIZATION_TYPE_CODE_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'ORGANIZATION_TYPE_CODE' primary_key, 'SELECT COUNT(DISTINCT ORGANIZATION_TYPE_CODE) , {var1} FROM AUDIT.VAP_ORGANIZATION_TYPE_CODE_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 130 table_id, 'VAP_OUTBOUND_BATCH_STATUS_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'OUTBOUND_BATCH_STATUS' primary_key, 'SELECT COUNT(DISTINCT OUTBOUND_BATCH_STATUS) , {var1} FROM AUDIT.VAP_OUTBOUND_BATCH_STATUS_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 131 table_id, 'VAP_OUTBOUND_BATCH_TYPE_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'OUTBOUND_BATCH_TYPE' primary_key, 'SELECT COUNT(DISTINCT OUTBOUND_BATCH_TYPE) , {var1} FROM AUDIT.VAP_OUTBOUND_BATCH_TYPE_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 132 table_id, 'VAP_PHOENIXML_TO_VAP_RRC_XREF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'PHOENIX_RRC_ID' primary_key, 'SELECT COUNT(DISTINCT PHOENIX_RRC_ID) , {var1} FROM AUDIT.VAP_PHOENIXML_TO_VAP_RRC_XREF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 133 table_id, 'VAP_PHONE_TYPE_CODE_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'PHONE_TYPE_CODE' primary_key, 'SELECT COUNT(DISTINCT PHONE_TYPE_CODE) , {var1} FROM AUDIT.VAP_PHONE_TYPE_CODE_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 134 table_id, 'VAP_PRODUCT_DELIVERY_TYPE_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'PRODUCT_DELIVERY_TYPE_CODE' primary_key, 'SELECT COUNT(DISTINCT PRODUCT_DELIVERY_TYPE_CODE) , {var1} FROM AUDIT.VAP_PRODUCT_DELIVERY_TYPE_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 135 table_id, 'VAP_RECYCLE_PATTERN_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'PATTERN_ID' primary_key, 'SELECT COUNT(DISTINCT PATTERN_ID) , {var1} FROM AUDIT.VAP_RECYCLE_PATTERN_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 136 table_id, 'VAP_RESPONSE_REASON_TYPE_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'RESPONSE_REASON_TYPE' primary_key, 'SELECT COUNT(DISTINCT RESPONSE_REASON_TYPE) , {var1} FROM AUDIT.VAP_RESPONSE_REASON_TYPE_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 137 table_id, 'VAP_SESSION_REASON_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'SESSION_REASON' primary_key, 'SELECT COUNT(DISTINCT SESSION_REASON) , {var1} FROM AUDIT.VAP_SESSION_REASON_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 138 table_id, 'VAP_SESSION_STATUS_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'SESSION_STATUS' primary_key, 'SELECT COUNT(DISTINCT SESSION_STATUS) , {var1} FROM AUDIT.VAP_SESSION_STATUS_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 139 table_id, 'VAP_SESSION_TYPE_CODE_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'SESSION_TYPE_CODE' primary_key, 'SELECT COUNT(DISTINCT SESSION_TYPE_CODE) , {var1} FROM AUDIT.VAP_SESSION_TYPE_CODE_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 140 table_id, 'VAP_SHIPPING_CARRIER_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'SHIPPING_CARRIER_CODE' primary_key, 'SELECT COUNT(DISTINCT SHIPPING_CARRIER_CODE) , {var1} FROM AUDIT.VAP_SHIPPING_CARRIER_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 141 table_id, 'VAP_SHIPPING_METHOD_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'SHIPPING_METHOD_CODE' primary_key, 'SELECT COUNT(DISTINCT SHIPPING_METHOD_CODE) , {var1} FROM AUDIT.VAP_SHIPPING_METHOD_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 142 table_id, 'VAP_UI_PERF_REPORT_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'REPORT_ID' primary_key, 'SELECT COUNT(DISTINCT REPORT_ID) , {var1} FROM AUDIT.VAP_UI_PERF_REPORT_REF' sql
UNION ALL
SELECT 'AUDIT' schema_name, 143 table_id, 'RPT_INTERCHANGE_COUNTS' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'MERCHANT_ID,RECONCILED_DATE,RATE_ID,TXN_COUNT,TOTAL_TXN_COUNT' primary_key, 'SELECT COUNT(DISTINCT MERCHANT_ID,RECONCILED_DATE,RATE_ID,TXN_COUNT,TOTAL_TXN_COUNT) , {var1} FROM AUDIT.RPT_INTERCHANGE_COUNTS' sql
UNION ALL
SELECT 'AUDIT' schema_name, 144 table_id, 'RPT_PAGING_EVENT_SUMMARY' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'EVENT_MONTH,EVENT_YEAR,APPLICATION,EVENT_COUNT,CREATE_DATE' primary_key, 'SELECT COUNT(DISTINCT EVENT_MONTH,EVENT_YEAR,APPLICATION,EVENT_COUNT,CREATE_DATE) , {var1} FROM AUDIT.RPT_PAGING_EVENT_SUMMARY' sql
UNION ALL
SELECT 'AUDIT' schema_name, 145 table_id, 'VAP_UI_PERF_RESULTS' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'ORGANIZATION_ID,CREATE_DATE,HTTP_RESPONSE_CODE,TIMED_OUT,SERVER_URL,REQUEST_BODY,START_TIME,END_TIME' primary_key, 'SELECT COUNT(DISTINCT ORGANIZATION_ID,CREATE_DATE,HTTP_RESPONSE_CODE,TIMED_OUT,SERVER_URL,REQUEST_BODY,START_TIME,END_TIME) , {var1} FROM AUDIT.VAP_UI_PERF_RESULTS' sql
UNION ALL
SELECT 'AUDIT' schema_name, 146 table_id, 'VAP_UI_SESSION_EXPORT_STATS' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'INTERACTIVITY_STATS_ID,FIRST_SESSION_ID,FIRST_BATCH_ID,COUNT' primary_key, 'SELECT COUNT(DISTINCT INTERACTIVITY_STATS_ID,FIRST_SESSION_ID,FIRST_BATCH_ID,COUNT) , {var1} FROM AUDIT.VAP_UI_SESSION_EXPORT_STATS' sql
UNION ALL
SELECT 'AUDIT' schema_name, 147 table_id, 'VAP_UI_TXN_EXPORT_STATS' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'INTERACTIVITY_STATS_ID,TXN_TYPE,TXN_COUNT' primary_key, 'SELECT COUNT(DISTINCT INTERACTIVITY_STATS_ID,TXN_TYPE,TXN_COUNT) , {var1} FROM AUDIT.VAP_UI_TXN_EXPORT_STATS' sql
UNION ALL
SELECT 'AUDIT' schema_name, 148 table_id, 'VAP_FUNDING_SOURCE_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, null dates_ago, 'FUNDING_SOURCE' primary_key, 'SELECT COUNT(DISTINCT FUNDING_SOURCE) , {var1} FROM AUDIT.VAP_FUNDING_SOURCE_REF' sql
) a

---------------------------------------------------
import java.sql.Timestamp
println("CustomerMidMDBDriver::job is started")
    val start_time = new Timestamp(System.currentTimeMillis()).toString
    
    val end_time = new Timestamp(System.currentTimeMillis()).toString
    
var table_id = 1     
val summarized_date = "2019-02-10"
println("Table id is ")
println("sourceCount: "+ sourceCount)
println("dedupCount: "+ dedupCount)
println("PART_CAL_VAL: "+ job_properties("PART_CAL_VAL"))
println("coalesceCount: "+ coalesceCount)
      
org.apache.spark.sql.Row

 import org.apache.spark.sql._


val df_confsql = spark.sql("select table_name,sql from dev_audit.bda_tables_sumrz_conf where table_id = 1");

val row_confsql = df_confsql.collect.toList(0)

val table_name = row_confsql.getString(0)
val reflexed_sql = row_confsql.getString(1).replace("{var1}", "'"+ summarized_date +"'") 

val df_count = spark.sql(reflexed_sql)

val count = df_count.collect.toList(0).getLong(0)


val row_insert = Seq((table_id, table_name, summarized_date, count, start_time, end_time))

var data = spark.sqlContext.createDataFrame(row_insert).toDF("table_id", "table_name", "summarized_date", "count", "start_time", "end_time")  

data.show() 
data.write.options(Map("delimiter" -> "\u0007")).mode(SaveMode.Append).csv("/lake/audit/ecomm/bda_tables_sumrz_data")

spark.sql("select * from dev_audit.bda_tables_sumrz_data").show()

df_count.rdd.isEmpty

val count = df_count.collect.toList 

r.getInt(0), r.getInt(1)


df_count.head(1)
