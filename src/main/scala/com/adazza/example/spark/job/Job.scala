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

    val c = counts.toDF()

    c.show(truncate = false)
    c.describe()

    counts.saveAsTextFile(output)

  }
}

