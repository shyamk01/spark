import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType

object SparkSessionTest extends App{
	val spark = SparkSession.builder()
		.master("local[5]")
		.appName("SparkByExample")
		.getOrCreate();
	spark.sparkContext.setLogLevel("ERROR")

	// val rdd3=spark.sparkContext.textFile("C:/tmp/abc.csv")
	// val rdd6 = rdd3.map(f=>f.split(",")).take(10)
	//rdd6.foreach(f=>println(f(0), f(1)))
	// println(rdd6)

	/*val rdd = spark.sparkContext.parallelize(Seq(("Java",2000),("Python",10000),("Scala",3000)))
	val rdd3= rdd.map(row=>{(row._1,row._2+100)})
	val myrdd=spark.range(20).toDF().rdd
	println(myrdd)
	rdd3.foreach(println)
*/
  val simpleData = Seq(("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  )
  import spark.implicits._
//
//  val df = simpleData.toDF("employee_name","department","state","salary","age","bonus")
//
//  val df2 = df.groupBy("state").count()
//
//  println(df2.rdd.getNumPartitions)
//  case class Person(name: String, age:Int)
//  val data = Seq(new Person("A", 25), new Person("B", 30))
//
//  val rdddata= spark.sparkContext.makeRDD(data)
//  val dfdata = rdddata.toDF()
//  val ds = dfdata.as[Person]
  val rdd_flat = spark.sparkContext.parallelize(Seq("Roses are red", "Violets are blue"))  // lines

  rdd_flat.collect().foreach(println)
  println("shyam")
  rdd_flat.map(_.length).collect().foreach(println)
  rdd_flat.flatMap(_.split(" ")).collect().foreach(println)
  val primitiveDS = Seq(1, 2, 3).toDS()

 primitiveDS.map(_ + 1).collect().foreach(println) // Returns: Array(2, 3, 4)

  //ds.filter(x=>x.age>25).show()
  // dfdata.filter(x=>x._1>25)
//  val rdd = spark.sparkContext.parallelize(List("hello","world","good","morning"))
//  val pairRdd = rdd.map(a => (a.length,a))
//  //pairRdd.collect().foreach(println)
//
//  val groupbyKey = pairRdd.groupByKey().sortByKey()
//  groupbyKey.collect().foreach(println)
  /*val rddRange = spark.sparkContext.parallelize(Range(0,25),6)
  println("From Local[5]"+ rddRange.partitions.size)
  rddRange.foreach(println)
  val rdd31 = rddRange.coalesce(4)
  println("Repartition size : "+rdd31.partitions.size)

  rdd31.saveAsTextFile("c:/tmp/text04.txt")

  val rdd:RDD[String]=spark.sparkContext.textFile("c:/tmp/text01.txt")
  rdd.coalesce(3)
  println("initial partition count:" + rdd.getNumPartitions)
  //rdd.collect().foreach(println)

  val rdd2:RDD[String] = rdd.flatMap(f=>f.split(" "))
  //rdd2.foreach(f=>println(f))

  val rdd3:RDD[(String,Int)] = rdd2.map(m=>(m,1))
  //rdd3.foreach(println)

  val rdd4=rdd3.filter(a=>a._1.startsWith("a"))
  //rdd4.foreach(println)

  val rdd5 = rdd3.reduceByKey(_+_)
  //rdd5.foreach(println)

  val rdd6 = rdd5.map(a=>(a._2,a._1)).sortByKey()
  // rdd6.foreach(println)
  //println(rdd6.count())

  val listRdd = spark.sparkContext.parallelize(List(1,2,3,4,5,3))
  // println("countApprox : "+listRdd.countApprox(100))
  val rdd1 = spark.sparkContext.parallelize(List("Germany India USA","USA India Russia","India Brazil Canada China"))
  val wordRdd = rdd1.flatMap(_.split(" "))
  val pairRdd = wordRdd.map(f=>(f,1))
  println(wordRdd)*/
}
