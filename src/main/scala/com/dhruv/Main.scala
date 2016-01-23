package com.dhruv

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext

object Main {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Spark to Hive examples")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val df = hiveContext.read.json("file:///usr/hdp/current/spark-client/examples/src/main/resources/people.json")
    df.write.mode(SaveMode.Overwrite).format("orc").saveAsTable("novetta_people")
  }
}
