package com.clearscore.test

import org.apache.spark.sql.SparkSession

/** Common methods for ETL tasks.
 */

object etlcommon extends App {

  /** Creates a Spark session.
   * @return a SparkSession.
   */

    def createSparkSession : SparkSession ={
      val spark = SparkSession.builder()
        .master("local[1]")
        .appName("ClearScore case study")
        .getOrCreate()

      spark
    }
}
