package com.github.valrcs.spark

import org.apache.spark.sql.SparkSession

object SimpleApp {
  def main(args: Array[String]) {
    if (args.length < 2) {
      println("Not enough arguments!")
      return
    }
//    val logFile = "C:/temp/donuts.txt" // Should be some file on your system
    val logFile = args(0) // Should be some file on your system
    val needle = args(1)
    val spark = SparkSession.builder.appName("Simple Application").config("spark.master", "local").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains(needle)).count()
    println(s"You got $numAs $needle(s)")
  }

}
