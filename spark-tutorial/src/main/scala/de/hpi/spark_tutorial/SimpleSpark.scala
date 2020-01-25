package de.hpi.spark_tutorial

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SimpleSpark extends App {

  override def main(args: Array[String]): Unit = {
    var path = "TPCH"
    var cores = 4

    if (args.length == 0) println("No arguments passed. Use default --path TPCH and --cores 4")
    else {
      val arglist = args.toList

      def parseArgs(list: List[String]) : Int = {
        list match {
          case Nil => 0
          case "--path" :: value :: tail =>
            path = value
            parseArgs(tail)
          case "--cores" :: value :: tail =>
            cores = value.toInt
            parseArgs(tail)
          case option :: tail => println("Unknown option "+option)
            sys.exit(1)
        }
      }
      parseArgs(arglist)
    }


    // Turn off logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    //------------------------------------------------------------------------------------------------------------------
    // Setting up a Spark Session
    //------------------------------------------------------------------------------------------------------------------

    // Create a SparkSession to work with Spark
    val sparkBuilder = SparkSession
      .builder()
      .appName("SparkTutorial")
      .master(s"local[$cores]") // local, with 4 worker cores
    val spark = sparkBuilder.getOrCreate()

    // Set the default number of shuffle partitions (default is 200, which is too high for local deployment)
    spark.conf.set("spark.sql.shuffle.partitions", "8") //

    /*
    def time[R](block: => R): R = {
      val t0 = System.currentTimeMillis()
      val result = block
      val t1 = System.currentTimeMillis()
      println(s"Execution: ${t1 - t0} ms")
      result
    }
    */

    //------------------------------------------------------------------------------------------------------------------
    // Inclusion Dependency Discovery (Homework)
    //------------------------------------------------------------------------------------------------------------------

    val inputs = List("region", "nation", "supplier", "customer", "part", "lineitem", "orders")
      .map(name => s"data/$path/tpch_$name.csv")

    Sindy.discoverINDs(inputs, spark)
  }
}
