// Databricks notebook source
//dbfs:/FileStore/shared_uploads/sharanyakp27@gmail.com/access_log_Jul95

// COMMAND ----------

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// COMMAND ----------

// Disable Logs
Logger.getLogger("org").setLevel(Level.OFF)
val spark = SparkSession.builder().appName("WebLog").master("local[*]").getOrCreate()

// COMMAND ----------

import spark.implicits._

// COMMAND ----------

val logs_DF = spark.read.text("dbfs:/FileStore/shared_uploads/sharanyakp27@gmail.com/access_log_Jul95")
logs_DF.printSchema()

// COMMAND ----------

// Display top 5 rows of data
logs_DF.show(5, false)

// COMMAND ----------

// Split Data in an organised way as follows: remote host, timestamp, request, endpoint, protocol, status, content size
logs_DF.select(regexp_extract($"value","""([^(\s|,)]+)""", 1).alias("host")).show()

// COMMAND ----------

// Split Data in an organised way as follows: remote host, timestamp, request, endpoint, protocol, status, content size
val hosts = logs_DF.select(regexp_extract($"value","""([^(\s|,)]+)""", 1).alias("host"))
hosts.show()

// COMMAND ----------

// Extract TimeStamp or Date&Time [01/Jul/1995:00:00:01 -0400]
val timestamp = logs_DF.select(regexp_extract($"value", """\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]""",1).alias("Timestamp"))
timestamp.show()

// COMMAND ----------

// Extract HOST - "GET /history/apollo/ HTTP/1.0"
val host = logs_DF.select(regexp_extract($"value", """\"(\S+)\s(\S+)\s*(\S*)\"""", 1).alias("Method"))
host.show()

// COMMAND ----------

// Extract Endpoint - "GET /history/apollo/ HTTP/1.0"
val endpoint = logs_DF.select(regexp_extract($"value", """\"(\S+)\s(\S+)\s*(\S*)\"""", 2).alias("Endpoint"))
endpoint.show(false)

// COMMAND ----------

// Extract Protocol - "GET /history/apollo/ HTTP/1.0"
val protocol = logs_DF.select(regexp_extract($"value", """\"(\S+)\s(\S+)\s*(\S*)\"""", 3).alias("Protocol"))
protocol.show(10, false)

// COMMAND ----------

// Extract HTTP Status - 200
// cast() - Typecasting
val status = logs_DF.select(regexp_extract($"value", """\s(\d{3})""", 1).cast("int").alias("Status"))
status.show()

// COMMAND ----------

// Extract Content Size 
val content = logs_DF.select(regexp_extract($"value", """\s(\d+)$""", 1).cast("int").alias("Content Size"))
content.show()

// COMMAND ----------

// Merge multiple regular expressions
val weblog_df = logs_DF.select(regexp_extract($"value","""([^(\s|,)]+)""", 1).alias("host"),
                           regexp_extract($"value", """\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]""",1).alias("Timestamp"),
                           regexp_extract($"value", """\"(\S+)\s(\S+)\s*(\S*)\"""", 1).alias("Method"),
                           regexp_extract($"value", """\"(\S+)\s(\S+)\s*(\S*)\"""", 2).alias("Endpoint"),
                           regexp_extract($"value", """\"(\S+)\s(\S+)\s*(\S*)\"""", 3).alias("Protocol"),
                           regexp_extract($"value", """\s(\d{3})""", 1).alias("Status"),
                           regexp_extract($"value", """\s(\d+)$""", 1).alias("Content Size"))
weblog_df.show()

// COMMAND ----------

weblog_df.printSchema()

// COMMAND ----------

// Find Count of Null, None, NaN of all dataframe columns
import org.apache.spark.sql.functions.{col,when,count}
import org.apache.spark.sql.Column

// UDF
def countNullCols (columns:Array[String]):Array[Column] = {
   columns.map(c => {
   count(when(col(c).isNull, c)).alias(c)
  })
}

weblog_df.select(countNullCols(weblog_df.columns): _*).show()

// COMMAND ----------

// Convert Textfile Format to Parquet File Format
weblog_df.write.parquet("dbfs:/FileStore/shared_uploads/sharanyakp27@gmail.com/weblogs/")

// COMMAND ----------

// Read Parquet File Format
val parquetLogs = spark.read.parquet("dbfs:/FileStore/shared_uploads/sharanyakp27@gmail.com/weblogs/")
parquetLogs.show()

// COMMAND ----------

// cache() - In Memory Storage
parquetLogs.cache()

// COMMAND ----------

// Persistence Storage Levels - MEMORY_ONLY, MEMORY_AND_DISK, DISK_ONLY, MEMORY_AND_DISK_SER
import org.apache.spark.storage.StorageLevel
val parquetLogsDF = parquetLogs.persist(StorageLevel.MEMORY_AND_DISK)

// COMMAND ----------

// TYpecast the timestamp column to Date
parquetLogsDF.select(to_date($"Timestamp")).show(5)

// COMMAND ----------

val month_map = Map("Jan" -> 1, "Feb" -> 2, "Mar" -> 3, "Apr" -> 4, "May" -> 5, "Jun" -> 6, "Jul" -> 7, "Aug" -> 8, "Sep" -> 9,
                   "Oct" -> 10, "Nov" -> 11, "Dec" -> 12)
// UDF 
def parse_time(s : String):String = {"%3$s-%2$s-%1$s %4$s:%5$s:%6$s".format(s.substring(0,2), month_map(s.substring(3,6)), s.substring(7,11), s.substring(12,14), s.substring(15,17), s.substring(18))
}

val toTimestamp = udf[String, String](parse_time(_))

val parquetlogsDF = parquetLogs.select($"*", to_timestamp(toTimestamp($"Timestamp")).alias("time")).drop("Timestamp")
parquetlogsDF.show()

// COMMAND ----------

// Describe the Status
parquetlogsDF.describe(cols = "Status").show()
// HTTP Status Analysis - Frequency
parquetlogsDF.groupBy("Status").count().sort(desc("count")).show()

// COMMAND ----------

// HTTP Status Analysis - Frequency
parquetlogsDF.groupBy("Status").count().sort(desc("count")).show()

// COMMAND ----------

parquetlogsDF.groupBy("host").count().sort(desc("count")).show()

// COMMAND ----------

// Frequency of Path Visits in July Month
parquetlogsDF.groupBy("Endpoint").count().sort(desc("count")).show(false)

// COMMAND ----------

// Frequency of Path with Minimum Visit Count > 10
parquetlogsDF.groupBy("Endpoint").count().filter($"count" > 10).sort(desc("count")).show(false)

// COMMAND ----------

// Top 20 Error Paths - Status != 200
parquetlogsDF.filter($"Status" =!= 200).groupBy("Endpoint").count().sort(desc("count")).show(false)

// COMMAND ----------

parquetlogsDF.filter($"Status" === 200).groupBy("Endpoint").count().sort(desc("count")).show(10)
// Top 10 Paths - with Status == 200

// COMMAND ----------

// Unique Hosts Count
val unique_host = parquetlogsDF.select("host").distinct().count()
println(unique_host)

// COMMAND ----------

val dailyhost = parquetlogsDF.withColumn("day", dayofyear($"time")).withColumn("year", year($"time")).select("host", "day", "year").distinct().groupBy("day", "year").count().sort("year", "day")

dailyhost.show(5)

// COMMAND ----------

val daily_hosts = parquetlogsDF.withColumn("day",dayofyear($"time")).withColumn("year",year($"time"))                   
daily_hosts.show(5)

// COMMAND ----------

daily_hosts.createOrReplaceTempView("weblogsTable")

// COMMAND ----------

spark.sql("select * from weblogsTable limit 10").show()

// COMMAND ----------

spark.sql("select Status, count(*) as Count from weblogsTable where Status = 404 group by Status").show()

// COMMAND ----------

// Query to Display Distinct Path responding 404 in status error 
spark.sql("select distinct(Endpoint) from weblogsTable where Status = 404").show(false)

// COMMAND ----------

// List Top 20 Paths (Endpoint) with 404 Response Status Code
spark.sql("select distinct(Endpoint) from weblogsTable where Status = 404").show()

// COMMAND ----------

// List Top 20 Paths (Endpoint) with 404 Response Status Code
spark.sql("select Endpoint, count(*) as Count from weblogsTable where Status = 404 group by Endpoint order by Count Desc limit 20").show(false)

// COMMAND ----------

// List Top 20 Host with 404 Response Status Code
spark.sql("select host, count(*) as Count from weblogsTable where Status = 404 group by host order by Count Desc limit 20").show(false)

// COMMAND ----------

// MAGIC %sql
// MAGIC select host, count(*) as Count from weblogsTable where Status = 404 group by host order by Count Desc limit 20

// COMMAND ----------

spark.sql("select day,year count(*) as Count from weblogsTable where Status = 404 group by day, year order by Count desc limit 20").show(false)

// COMMAND ----------

// Display the List of 404 Error Response Status Code per Day
spark.sql("select year, day, count(*) as Count from weblogsTable where Status = 404 group by year, day order by day limit 30").show(31, false)


// COMMAND ----------

// Templete to Create Scala Project File
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WebLogsAnalysis {
  def main (args: Array[String]): Unit = {
      
  }
}

WebLogsAnalysis.main(null)

// COMMAND ----------

// Example Scala Project Code
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WebLogsAnalysis {
  def main (args: Array[String]): Unit = {
      // Disable Logs
      Logger.getLogger("org").setLevel(Level.OFF)
      val spark = SparkSession.builder().appName("WebLog").master("local[*]").getOrCreate()
    
      import spark.implicits._

      val logs_DF = spark.read.text("dbfs:/FileStore/shared_uploads/sharanyakp27@gmail.com/access_log_Jul95")
      logs_DF.printSchema()

      // Display top 5 rows of data
      logs_DF.show(5, false)

      // Merge multiple regular expressions
      val weblog_df = logs_DF.select(regexp_extract($"value","""([^(\s|,)]+)""", 1).alias("host"),
                           regexp_extract($"value", """\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]""",1).alias("Timestamp"),
                           regexp_extract($"value", """\"(\S+)\s(\S+)\s*(\S*)\"""", 1).alias("Method"),
                           regexp_extract($"value", """\"(\S+)\s(\S+)\s*(\S*)\"""", 2).alias("Endpoint"),
                           regexp_extract($"value", """\"(\S+)\s(\S+)\s*(\S*)\"""", 3).alias("Protocol"),
                           regexp_extract($"value", """\s(\d{3})""", 1).cast("int").alias("Status"),
                           regexp_extract($"value", """\s(\d+)$""", 1).cast("int").alias("Content Size"))
      weblog_df.show()
    
      weblog_df.printSchema()


      // Find Count of Null, None, NaN of all dataframe columns
      import org.apache.spark.sql.functions.{col,when,count}
      import org.apache.spark.sql.Column

      // UDF
      def countNullCols (columns:Array[String]):Array[Column] = {
         columns.map(c => {
         count(when(col(c).isNull, c)).alias(c)
        })
      }

      weblog_df.select(countNullCols(weblog_df.columns): _*).show()

      // Read Parquet File Format
      val parquetLogs = spark.read.parquet("dbfs:/FileStore/shared_uploads/sharanyakp27@gmail.com/weblogs/")
      parquetLogs.show()

      // cache() - In Memory Storage
      parquetLogs.cache()

      // Persistence Storage Levels - MEMORY_ONLY, MEMORY_AND_DISK, DISK_ONLY, MEMORY_AND_DISK_SER
      import org.apache.spark.storage.StorageLevel
      val parquetLogsDF = parquetLogs.persist(StorageLevel.MEMORY_AND_DISK)

  }
}

WebLogsAnalysis.main(null)

// COMMAND ----------


