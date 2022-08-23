package com.clearscore.casestudy.tests

import com.clearscore.test.etlcommon.createSparkSession
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfter, ScalaTestVersion}
import org.scalatest.funsuite.AnyFunSuite
import com.clearscore.test.run_exe._


class def_tests extends AnyFunSuite with BeforeAndAfter{

  val spark: SparkSession = createSparkSession

  test("average credit score should match") {

    val inputDF = spark.read.json("/Users/C81447B/Documents/Personal/Clearscore_Test/test_input/def1_input/*")

    val reportDF = question1(inputDF)

    val leftValue = reportDF.count()
    val rightValue = 4
    assert(leftValue === rightValue)

  }


  test("Number of users should match by employment status") {

    val inputDF = spark.read.json("/Users/C81447B/Documents/Personal/Clearscore_Test/test_input/def2_input/*")

    val accountDF = question2(inputDF)
    accountDF.show()

    val leftValue = accountDF.count()
    val rightValue = 1
    assert(leftValue === rightValue)

  }


  test("total number of score ranges should match") {

    val inputDF = spark.read.json("/Users/C81447B/Documents/Personal/Clearscore_Test/test_input/def3_input/*")

    val scoreDF = question3(inputDF)
    scoreDF.show()

    val leftValue = scoreDF.count()
    val rightValue = 4
    assert(leftValue === rightValue)
  }

  test("total number of joined records should match") {

    val accountDF = spark.read.json("/Users/C81447B/Documents/Personal/Clearscore_Test/test_input/def4_input/accounts/*")
    val reportDF = spark.read.json("/Users/C81447B/Documents/Personal/Clearscore_Test/test_input/def4_input/reports/*")


     val resultDF = question4(reportDF,accountDF)

    resultDF.show()

    val leftValue = resultDF.count()
    val rightValue = 3
    assert(leftValue === rightValue)

  }

}
