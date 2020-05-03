package com.fis.ingest

import java.sql.Timestamp

import org.apache.spark.sql.{Row, SQLContext, SaveMode, SparkSession, _}
import org.apache.spark.SparkContext



object EcommCountStar {

  def save_data(row_insert:  Seq[(String, String, String, String, String, String, String, String, String)] , spark: SparkSession, saved_path: String) : Unit =
    {
      import spark.implicits._
      val rdd = spark.sparkContext.parallelize(row_insert)
      var data = row_insert.toDF("table_id", "table_name", "summarized_date", "runtime_sql", "application_id", "count", "start_time", "end_time","log")
      data.show()
      data.write.options(Map("delimiter" -> "\u0007")).mode(SaveMode.Append).csv(saved_path)
    }

  def main(args: Array[String]): Unit = {


    val start_time = new Timestamp(System.currentTimeMillis()).toString
    printf("EcommCountStar::job is started at %s", start_time)
    if (args.length == 0) {
      println("need 2 parameters")
      return;
    }

    var table_id = args(0)
    val summarized_date = args(1)
    val saved_path = args(2)

    printf(" for table Id %s with summarized_date %s", table_id, summarized_date)

    val spark: SparkSession  = SparkSession.builder()
      //.master("local[1]")
      .appName("EcommCountStar")
      .config("spark.debug.maxToStringFields", 100)
      .config("spark.scheduler.mode", "FAIR")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")
// .enableHiveSupport()

    var application_id = spark.sparkContext.getConf.getAppId
    println("Spark application Id: "+ application_id)

      import spark.sqlContext.implicits._
try  {
      val df_confsql = spark.sql("select table_name,sql from dev_audit.bda_tables_sumrz_conf where table_id = "+table_id);
      val row_confsql = df_confsql.collect.toList(0)
      val table_name = row_confsql.getString(0)
      val runtime_sql = row_confsql.getString(1).replace("{var1}", "'"+ summarized_date +"'")
      try {
        val df_count = spark.sql(runtime_sql)
         
        val cnt = if (df_count.head(1).length == 0) "0" else df_count.collect.toList(0).getString(0)
        
        val end_time = new Timestamp(System.currentTimeMillis()).toString
        val row_done = Seq((table_id, table_name, summarized_date, runtime_sql, application_id, cnt, start_time, end_time, "DONE"))
        save_data(row_done, spark, saved_path)
        printf("EcommCountStar::job is completed at %s", end_time)
      } catch {
            case e: Throwable =>
            println(e)
              val end_time = new Timestamp(System.currentTimeMillis()).toString
              printf("EcommCountStar::job is failed at %s", end_time)
              val row_failed = Seq((table_id, table_name, summarized_date, runtime_sql, application_id, null, start_time, end_time, "ERROR: "+ e.getMessage))
              save_data(row_failed, spark, saved_path )
              //System.exit(1)

} catch {
  case e: Throwable =>
    println(e)
    val end_time = new Timestamp(System.currentTimeMillis()).toString
    printf("EcommCountStar::job is failed at %s", end_time)
    val row_failed_2 = Seq((table_id, "", summarized_date, "", application_id, null, start_time, end_time, "ERROR: not binding values yet + "+ e.getMessage))
    save_data(row_failed_2, spark, saved_path)
    
} finally {
  spark.stop()
}


    //
   /* val spark: SparkSession = SparkSession.builder().master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()
    val rdd: RDD[Int] = spark.sparkContext.parallelize(List(1, 2, 3, 4, 5))
    val rddCollect: Array[Int] = rdd.collect()
    println("Number of Partitions: " + rdd.getNumPartitions)
    println("Action: First element: " + rdd.first())
    println("Action: RDD converted to Array[Int] : ")
    rddCollect.foreach(println)*/
  }

}
