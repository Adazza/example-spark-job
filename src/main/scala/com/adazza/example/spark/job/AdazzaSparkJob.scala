package com.adazza.example.spark.job

import org.apache.spark.sql.SparkSession
import scala.concurrent.ExecutionContext

/**
  * Trait that define a spark job that will be run as part of V1 ingestion backend system.
  */
trait AdazzaSparkJob extends Serializable {
  /**
    * Method that runs the spark job's code.
    *
    * @param sparkSession SparkSession spark session contain sqlContext and sparkContext
    * @param inputFiles Array[String] list of input files to read
    * @param outputFile String output file/glob
    * @param jobArgs Array[String] arguments passing into the job
    */
  def run(sparkSession: SparkSession,
          inputFiles: Array[String],
          outputFile: String,
          jobArgs: Array[String])
}
