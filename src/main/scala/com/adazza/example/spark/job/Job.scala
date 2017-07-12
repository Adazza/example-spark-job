package com.adazza.example.spark.job

import org.apache.spark.sql.SparkSession

/** word count */
object Job {
  def main(args: Array[String]) {

    val input = args(0)
    val output = args(1)

    val sc = SparkSession
      .builder
      .appName("Adazza Example JOb")
      .getOrCreate()

    import sc.implicits._

    val textFile = sc.sparkContext.textFile(input)
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    val c = counts.toDF("word", "count")

    c.show(truncate = false)
    c.describe("word")
    c.describe("count")

    c.write
      .option("sep", ",")
      .option("quote", "\"")
      .option("escape", "\"")
      .option("nullValue", "")
      .option("dateFormat", "yyyy-MM-dd")
      .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSZZ")
      .option("header", value = true)
      .option("compression", "gzip")
      .mode("overwrite")
      .csv(output)
  }
}

