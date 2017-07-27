package com.adazza.example.spark.job

import java.io.File

import org.scalatest.FlatSpec

import java.util.UUID

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

/**
  * Example Spark Job unit test that utilizes the SparkJobTestHarness
  */
@RunWith(classOf[JUnitRunner])
class TestWordCount extends FlatSpec with SparkJobTestHarness {

  // Helper method that created an 8-character string from a random UUID
  private def uuid8: String = UUID.randomUUID().toString.take(8)

  // Helper method that takes a directory and returns a list of it's files
  private def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List.empty[File]
    }
  }

  "WordCount" should "write output file to correct location" in {
    val inputAbsPath = getClass.getResource("/test-files/usdeclar.txt").getPath
    val outputAbsPath = s"/tmp/spark-test-harness-$uuid8"
    val input = s"file://$inputAbsPath"
    val output = s"file://$outputAbsPath"
    val jobArgs = Array.empty[String]

    WordCount.run(sparkSession, Array(input), output, jobArgs)

    // scalatest concurrent and time usage
    eventually(timeout(2 seconds)) {
      assertResult(true) {
        getListOfFiles(outputAbsPath).nonEmpty
      }
    }
  }
}
