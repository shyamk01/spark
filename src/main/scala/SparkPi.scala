import org.apache.log4j.{Level, Logger}

import scala.math.random
import org.apache.spark.{SparkConf, SparkContext}
// The above data are drawer open and closed events from a cash register.  The customers want to use our big data platform to calculate the hourly open and close events of the drawers.

//If you have more experience with batch processing using big data write the following answers using a batch API/methodology.  Assume that the file is dropped nightly and the job will run on a json file with the above contents.

//If you have experience with spark streaming API, assume the data from this customers is sent with a contractual lag time of no more than 5 seconds.  You can assume the data is being sent to Kafka with a single event as a message.

//Question 1: Compute the number of “open” events and “close” events for each one hour window by customer.  Please note that the date-time field is a unique format.

//Question 2: After the calcuation is complete requriements dictate that an external hive table be created/updated once an hour. Feel free to write the file in any format and show your work to register the table.

//Question 3: This time compute the number of "open" and "close" events for each one hour window by zip code.  You will have to parse the address to get the zip code and can assume the data is is always in the extact string format as above.


object SparkPi {
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("Spark Pi")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val slices = if (args.length > 0) args(0).toInt else 2
    val n = 100000 * slices
    val count = sc.parallelize(1 to n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)
    println("pi is roughly" + 4.0 * count / n)
    sc.stop()

  }
}
