import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType,StringType,IntegerType}



object Spark_Test extends App {
  val sparkConf=new SparkConf()
                  .setMaster("local[*]")
                  .setAppName("SparkConf")

  val spark= SparkSession.builder().config(sparkConf).getOrCreate()
  val sc=spark.sparkContext
  sc.setLogLevel("ERROR")
  val csvFile = "departuredelays.csv"
  val schema="date STRING, delay INT, distance INT, origin STRING, destination STRING"
  val df = spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .option("schema", schema)
    .load(csvFile)

 df.createOrReplaceTempView("us_delay_flights_tbl")

 df.printSchema()
 spark.sql(f"""
      CREATE OR REPLACE GLOBAL TEMP VIEW us_origin_airport_SFO_global_tmp_view AS
      SELECT date, delay, origin, destination from us_delay_flights_tbl WHERE
      origin = 'SFO' """)
  val df_sfo = spark.sql(f"""SELECT date, delay, origin, destination FROM
    us_delay_flights_tbl WHERE origin = 'SFO'""")
  val df_jfk = spark.sql(f"""SELECT date, delay, origin, destination FROM
    us_delay_flights_tbl WHERE origin = 'JFK'""")

  // Create a temporary and global temporary view
  df_sfo.createOrReplaceGlobalTempView("us_origin_airport_SFO_global_tmp_view")
  df_jfk.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")
  spark.sql("SELECT *,FLOOR(RAND(123456)*19) FROM us_origin_airport_JFK_tmp_view").show(10)

 /*spark.sql("CREATE DATABASE learn_spark_db")
 spark.sql("USE learn_spark_db")
 spark.sql(f"""CREATE TABLE us_delay_flights_tbl(date STRING, delay INT,
  distance INT, origin STRING, destination STRING)
  USING csv OPTIONS (PATH
  'departuredelays.csv')""")*/

  /*(df.select("distance", "origin", "destination")
    .where("distance > 1000")
    .orderBy("distance").show(10))*/

  /*spark.sql("CREATE DATABASE learn_spark_db")
  spark.sql("USE learn_spark_db")
  spark.sql(f"""CREATE TABLE managed_us_delay_flights_tbl (date STRING, delay INT,distance INT, origin STRING, destination STRING)""")*/
}
