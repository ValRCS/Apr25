package com.github.valrcs.spark

import org.apache.spark.sql.SparkSession

case class Donut(donut_name:String, quantity_purchased:BigInt, price:Double)

object FilterJSON {
  def main(args: Array[String]) :Unit = {
    //TODO check args
    val spark = SparkSession.builder.appName("JSON Filter").config("spark.master", "local").getOrCreate()
    import spark.implicits._
    val df = spark.read.option("multiline", "true").json("c:/temp/donuts.json")
    df.createOrReplaceTempView("donview")
    val expensive = spark.sql("SELECT * FROM donview WHERE price > 2")
    println(expensive.show)
    //with implicits we can mold data to our liking and cast each row to our class (Donut) in this case
    expensive.as[Donut].take(2).foreach(d => println(s"name=${d.donut_name}, quant=${d.quantity_purchased}, price=${d.price}"))
//    expensive.map(cols => s"name=${cols(0)}, price=${cols(2)}").foreach(println)

    //    TODO find how to force overwrite of .csv
    //    expensive.write.option("header","true").csv("c:/temp/expensive.csv")

  }

}
