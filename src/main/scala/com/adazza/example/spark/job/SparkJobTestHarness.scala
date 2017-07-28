package com.adazza.example.spark.job

import java.util.UUID

import org.apache.spark.sql.SparkSession
import org.scalatest._

/**
  * Test Harness for running Adazza Spark Jobs locally.
  */
trait SparkJobTestHarness extends TestSuite { this: Suite =>

  var sparkSession = createSparkSession
  def output = s"spark-test-harness-$uuid8"

  // Helper method that created an 8-character string from a random UUID
  private def uuid8: String = UUID.randomUUID().toString.take(8)

  // Helper method to create a spark session
  private def createSparkSession: SparkSession = {
    val localMaster = "local[*]"
    val harnessAppName = s"Spark-Job-Test-Harness-$uuid8"

    SparkSession.builder()
      .master(localMaster)
      .appName(harnessAppName)
      .getOrCreate()
  }


  abstract override def withFixture(test: NoArgTest): Outcome = {
    try {
      sparkSession = createSparkSession
      super.withFixture(test)
    } finally
      sparkSession.close
  }
}
