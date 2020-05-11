package com.fis.ingest

import java.sql.Timestamp

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Row, SQLContext, SaveMode, SparkSession, _}
import org.apache.spark.SparkContext

object EcommCountStar {

  def main(args: Array[String]): Unit = {


    val start_time = new Timestamp(System.currentTimeMillis()).toString
    printf("EcommCountStar::job is started at %s", start_time)
    if (args.length != 4) {
      println("It needs 4 parameters: tableID tableName Date savedPath")
      System.exit(1)
    }

    var table_id = args(0)
    var table_name = args(1)
    var summarized_date = args(2)
    var saved_path = args(3)

    printf("\nFor table Id %s %s with summarized_date %s", table_id, table_name, summarized_date)

    val spark: SparkSession = SparkSession.builder()
      .appName("EcommCountStar")
      .config("spark.debug.maxToStringFields", 100)
      .config("spark.scheduler.mode", "FAIR")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")

    var application_id = spark.sparkContext.getConf.getAppId
    println("Spark application Id: " + application_id)


    try {
      printf("\nRead the query for table_id %s %s from table audit.bda_tables_sumrz_conf: \n",table_id, table_name)
      val df_confsql = spark.sql("select sql from dev_audit.bda_tables_sumrz_conf where table_id = " + table_id);
      val row_confsql = df_confsql.collect.toList(0)
      //val table_name = row_confsql.getString(0)
      val runtime_sql = row_confsql.getString(0).replace("{var1}", "'" + summarized_date + "'")
      println("runtime_sql: "+runtime_sql)
      try {
        val df_count = spark.sql(runtime_sql)
        val cnt = if (df_count.head(1).length == 0) "0" else ""+df_count.collect.toList(0).getLong(0)
        val end_time = new Timestamp(System.currentTimeMillis()).toString
        val row_done = Seq((table_id, table_name, summarized_date, runtime_sql, application_id, cnt, start_time, end_time, "DONE"))
        save_data(row_done, spark, saved_path)
        printf("\nEcommCountStar::job is completed at %s", end_time)
      }
      catch {
        case e: Exception => {
          println("Exception: "+e)
          val end_time = new Timestamp(System.currentTimeMillis()).toString
          val row_failed = Seq((table_id, table_name, summarized_date, runtime_sql, application_id, null, start_time, end_time, "ERROR: " + StringUtils.left(e.printStackTrace.toString.replaceAll("\n\\s*\\|"," "),1000)))
          save_data(row_failed, spark, saved_path)
          printf("\nEcommCountStar::job is failed in inner try at %s", end_time)
          System.exit(1) }
      }
    }
    catch {
      case e: Exception => {
        println("Exception : "+e)
        val end_time = new Timestamp(System.currentTimeMillis()).toString
        val row_failed_2 = Seq((table_id, table_name, summarized_date, null, application_id, null, start_time, end_time, "ERROR: not binding value sql yet + " + StringUtils.left(e.printStackTrace.toString.replaceAll("\n\\s*\\|"," "),1000)))
        save_data(row_failed_2, spark, saved_path)
        printf("\nEcommCountStar::job is failed in outer try at %s", end_time)
        System.exit(1) }
    }
    finally {
      spark.stop()
    }
  }


  def save_data(row_insert: Seq[(String, String, String, String, String, String, String, String, String)], spark: SparkSession, saved_path: String): Unit = {
    printf("Start to save record %s into audit.bda_tables_sumrz_data", row_insert.mkString(" ~ "))
    import spark.implicits._
    val rdd = spark.sparkContext.parallelize(row_insert)
    var data = row_insert.toDF("table_id", "table_name", "summarized_date", "runtime_sql", "application_id", "count", "start_time", "end_time", "log")
    data.show()
    data.write.options(Map("delimiter" -> "\u0007")).mode(SaveMode.Append).csv(saved_path)
    println("Save record into audit.bda_tables_sumrz_data is completed.")
  }

}
