package com.adazza.example.spark.job

import org.apache.spark.sql.SparkSession

/**
  * Example Word Count Job that extends SparkJob trait
  */
object WordCount extends AdazzaSparkJob {
  override def run(spark: SparkSession,
                   inputFiles: Array[String],
                   outputFile: String,
                   jobArgs: Array[String]): Unit = {

    val inputFile = inputFiles(0)
    val sc = spark.sparkContext

    // Load our input data.
    val input =  sc.textFile(inputFile)

    // Split up into words.
    val words = input.flatMap(line => line.split(" "))

    // Transform into word and count.
    val counts = words.map(word => (word, 1))
      .reduceByKey { case (x, y) => x + y }
      .sortBy(f = { case (word, count) => count }, ascending = false)

    // Save the word count back out to a text file, causing evaluation.
    counts.saveAsTextFile(outputFile)
  }

  def main(args: Array[String]): Unit = {}
}
