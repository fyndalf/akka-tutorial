package de.hpi.spark_tutorial

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SimpleSpark extends App {

  override def main(args: Array[String]): Unit = {

    // Parameter parsing
    type OptionMap = Map[Symbol, Any]
    def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
      list match {
        case Nil => map
        case "--path" :: value :: tail => nextOption(map ++ Map('path -> value), tail)
        case "--cores" :: value :: tail => nextOption(map ++ Map('cores -> value.toInt), tail)
        case "--partitions" :: value :: tail => nextOption(map ++ Map('partitions -> value.toInt), tail)
        case string :: Nil => nextOption(map, Nil)
        case string :: tail => nextOption(map, tail)
      }
    }
    val options = nextOption(Map(), args.toList)

    val path = options.getOrElse('path, "data/TPCH")
    val cores = options.getOrElse('cores, 32)
    val partitions = options.getOrElse('partitions, 64)

    // Turn off logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Define a timer
    def time[R](block: => R): R = {
      val t0 = System.currentTimeMillis()
      val result = block
      val t1 = System.currentTimeMillis()
      println(s"Execution: ${t1 - t0} ms")
      result
    }

    //------------------------------------------------------------------------------------------------------------------
    // Setting up a Spark Session
    //------------------------------------------------------------------------------------------------------------------

    // Create a SparkSession to work with Spark
    val sparkBuilder = SparkSession
      .builder()
      .appName("SparkTutorial")
      .master(s"local[$cores]") // local, with $cores worker cores
    val spark = sparkBuilder.getOrCreate()

    // Set the default number of shuffle partitions (default is 200, which is too high for local deployment)
    spark.conf.set("spark.sql.shuffle.partitions", partitions.toString)

    val inputs = List("region", "nation", "supplier", "customer", "part", "lineitem", "orders")
      .map(name => s"$path/tpch_$name.csv")

    time {Sindy.discoverINDs(inputs, spark)}
  }
}
