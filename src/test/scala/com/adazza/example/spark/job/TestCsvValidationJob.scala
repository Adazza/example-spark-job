package com.adazza.example.spark.job

import java.io.File
import java.util.UUID

import org.apache.spark.SparkException
import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

/**
  * Unit testing the CSV Validation module to be used in a spark job.
  */
@RunWith(classOf[JUnitRunner])
class TestCsvValidationJob extends FlatSpec with SparkJobTestHarness {
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

  private def absInputPath(fileName: String) = getClass.getResource(s"/csv-validation/$fileName").getPath

  "CsvValidationJob" should "fail on an empty csv file" in {
    val inputFile = absInputPath("empty.csv")
    val input = s"file://$inputFile"

    assertThrows[UnsupportedOperationException] {
      CsvValidationJob.run(sparkSession, Array(input), output, Array.empty[String])
    }
  }

  it should "pass on a header only csv file" in {
    val inputFile = absInputPath("header-only.csv")
    val input = s"file://$inputFile"
    //val output = s"file://$outputAbsPath"

    CsvValidationJob.run(sparkSession, Array(input), output, Array.empty[String])
  }

  // TODO: extra work need to verify this
  ignore should "fail on header only csv file without quote" in {
    val inputFile = absInputPath("header-only-non-quoted.csv")
    val input = s"file://$inputFile"

    CsvValidationJob.run(sparkSession, Array(input), output, Array.empty[String])
  }

  /**
    * Throw RuntimeException when mode: FASTFAIL is set
    * Improperly fails. Tries to exactly "fit" the number of columns for mode: PERMISSIVE
    *  row: [1,2,3] 3
    * row: ["4";"5",6,null] 3
    */
  it should "fail on bad separator csv file" in {
    val inputFile = absInputPath("bad-separator.csv")
    val input = s"file://$inputFile"

    val caught = intercept[SparkException] {
      CsvValidationJob.run(sparkSession, Array(input), output, Array.empty[String])
    }

    println(caught.toString)
  }

  /**
    * FASTFAIL: Throws a runtime exception
    * PERMISSIVE
    */
  it should "fail on csv files with mismatching column counts" in {
    val input1 = s"file://${absInputPath("mismatch-column-number1.csv")}"
    val input2 = s"file://${absInputPath("mismatch-column-number2.csv")}"

    assertThrows[SparkException] {
      CsvValidationJob.run(sparkSession, Array(input1), output, Array.empty[String])
    }

    assertThrows[SparkException] {
      CsvValidationJob.run(sparkSession, Array(input2), output, Array.empty[String])
    }
  }

  /**
    * The parser doesn't seem to parse this correctly.
    * for example: "',"4""" gets parsed as valid when quote: " and escape: "
    * Any sequence of "..x..y.." where x has 1 " and at some point y contains , then it's read as malformed
    */
  it should "pass on a escaped fields in csv file" in {
    val inputFile = absInputPath("escaped-fields.csv")
    val input = s"file://$inputFile"

    CsvValidationJob.run(sparkSession, Array(input), output, Array.empty[String])
  }
//
//  it should "fail on a bad CRLF csv file (should be intrepreted as mismatching column size)" in {
//
//  }
}
