package com.adazza.example.spark.job

import org.apache.spark.sql.SparkSession
import org.scalatest._

/**
  * Test Harness for running Adazza Spark Jobs locally.
  */
trait SparkJobTestHarness extends TestSuite { this: Suite =>

  val sparkSession = createSparkSession

  // Helper method to create a spark session
  private def createSparkSession: SparkSession = {
    val localMaster = "local[*]"
    val harnessAppName = "Spark-Job-Test-Harness"

    SparkSession.builder()
      .master(localMaster)
      .appName(harnessAppName)
      .getOrCreate()
  }


  abstract override def withFixture(test: NoArgTest): Outcome = {
    try {
      super.withFixture(test)
    } finally
      sparkSession.stop
  }
}
