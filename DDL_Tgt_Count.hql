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
SELECT 'AUDIT' schema_name, 1 table_id, 'FE_PAYMENTS' table_name, 'TRAN' table_type, 'Y' count_ind, 'CREATED_DATE' summarized_date, 'PAYMENT_ID' primary_key, 'SELECT COUNT(DISTINCT PAYMENT_ID ) , TO_DATE(CREATED_DATE) FROM AUDIT.FE_PAYMENTS WHERE to_date(created_date) = {var1} and extract_date between {var1} and date_add({var1},3) group by to_date(created_date)' sql
UNION ALL
SELECT 'AUDIT' schema_name, 2 table_id, 'VAP_RESPONSE_REASON_CODE_REF' table_name, 'DIM' table_type, 'Y' count_ind, NULL summarized_date, 'RESPONSE_REASON_CODE' primary_key, 'SELECT COUNT(DISTINCT RESPONSE_REASON_CODE) , {var1} FROM AUDIT.vap_response_reason_code_ref' sql
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