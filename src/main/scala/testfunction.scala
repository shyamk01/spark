import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object testfunction extends  App{
  val sparkconf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("RemoveDataSkewness")

  val spark = SparkSession
    .builder()
    .config(sparkconf)
    .getOrCreate()
  val sc =spark.sparkContext
  sc.setLogLevel("ERROR")
  val studentRDD = sc.parallelize(Array(

    ("Joseph", "Maths", 83), ("Joseph", "Physics", 74), ("Joseph", "Chemistry", 91),

    ("Joseph", "Biology", 82), ("Jimmy", "Maths", 69), ("Jimmy", "Physics", 62),

    ("Jimmy", "Chemistry", 97), ("Jimmy", "Biology", 80), ("Tina", "Maths", 78),

    ("Tina", "Physics", 73), ("Tina", "Chemistry", 68), ("Tina", "Biology", 87),

    ("Thomas", "Maths", 87), ("Thomas", "Physics", 93), ("Thomas", "Chemistry", 91),

    ("Thomas", "Biology", 74), ("Cory", "Maths", 56), ("Cory", "Physics", 65),

    ("Cory", "Chemistry", 71), ("Cory", "Biology", 68), ("Jackeline", "Maths", 86),

    ("Jackeline", "Physics", 62), ("Jackeline", "Chemistry", 75), ("Jackeline", "Biology", 83),

    ("Juan", "Maths", 63), ("Juan", "Physics", 69), ("Juan", "Chemistry", 64),

    ("Juan", "Biology", 60)), 3)

 val words = Array("one","two","two","three","three","three")
 val wordsPairRDD = sc.parallelize(words).map(word=>(word,1))
 val wordCountWithReduce=wordsPairRDD.reduceByKey(_+_).collect()
  // wordCountWithReduce.foreach(println)
 val wordCountsWithGroup = wordsPairRDD.groupByKey().map(t=>(t._1,t._2.sum)).collect()
 wordCountsWithGroup.foreach(println)
}
