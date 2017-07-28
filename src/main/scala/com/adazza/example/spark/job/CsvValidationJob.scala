package com.adazza.example.spark.job
import org.apache.spark.sql.{DataFrameReader, Row, SparkSession}


/**
  * Job that verifies that the input files are "Adazza-strict" (stricter than RFC spec) CSV valid files.
  */
object CsvValidationJob extends AdazzaSparkJob {
  /**
    * Method that runs the spark job's code.
    *
    * @param sparkSession SparkSession spark session contain sqlContext and sparkContext
    * @param inputFiles   Array[String] list of input files to read
    * @param outputFile   String output file/glob
    * @param jobArgs      Array[String] arguments passing into the job
    */
  override def run(sparkSession: SparkSession,
                   inputFiles: Array[String],
                   outputFile: String,
                   jobArgs: Array[String]): Unit = {
    val reader = getReader(sparkSession)

    val df = reader.csv(inputFiles(0))

    val count = df.count

    val headerRow = df.schema.fields
    headerRow.foreach(f => print(s"[${f.name}] "))
    println(s" header count: ${headerRow.length}")
    df.foreach { row: Row =>
      for(i <- 0 to row.length-1) {
        print(s"[${row.get(i)}] ")
      }
      println(s"\nrow:${row.length}")
    }
  }

  /**
    * Returns a specific data frame reader
    *
    * @param spark SparkSession spark session to create a DF
    * @return DataFrameReader reader
    */
  def getReader(spark: SparkSession): DataFrameReader = {
    spark.read
      .option("encoding", "UTF-8")
      .option("sep", ",")
      .option("mode", "FAILFAST")
      .option("quote", "\"")
      .option("escape", "\"")
      .option("nullValue", "")
      .option("header", value = true)
  }
}
