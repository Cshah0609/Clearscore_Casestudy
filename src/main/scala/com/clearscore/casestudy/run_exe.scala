package com.clearscore.casestudy

import com.clearscore.casestudy.etlcommon.createSparkSession
import org.apache.spark.sql.{SparkSession,DataFrame}
import org.apache.spark.sql.functions.{col, _}


object run_exe {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = createSparkSession

    //Read Reports JSON File
    val reportsDF = readReportsJson(spark)

    //Read Accounts JSON File
    val accountsDF = readAccountJson(spark)

    //Attempted answers for all the test questions
     question1(reportsDF)
     question2(accountsDF)
     question3(reportsDF)
     question4(reportsDF,accountsDF)
  }

  def readReportsJson(spark: SparkSession): DataFrame = {

    val inputDF = spark.read.format("json")
      .option("multiLine", "true")
      .load("/Users/C81447B/Documents/Personal/Clearscore_Test/bulk-reports/reports/*/*/*")

    inputDF

  }


  def readAccountJson(spark: SparkSession): DataFrame = {

    val inputDF = spark.read.format("json")
      .option("multiLine", "true")
      .load("/Users/C81447B/Documents/Personal/Clearscore_Test/bulk-reports/accounts/*")

     inputDF

  }
  def question1(inputDF: DataFrame) :DataFrame = {

    //Parse Input Report DF to get Report->ScoreBlock->Delphi data
    val parseReportDF = inputDF
      .select("report-id","report.ScoreBlock.Delphi")
      .select(col("report-id"),explode(col("Delphi")).alias("newDelphi"))
      .select(col("report-id"),col("newDelphi.*"))

    parseReportDF.printSchema()
    parseReportDF.show()

    //Calculate average of credit score across all the reports.
    val avgCreditScore = parseReportDF
      .select(col("report-id"),col("Score").cast("int").as("Score"))
      .groupBy(col("report-id"))
      .avg("Score").alias("avg-score")

    avgCreditScore.printSchema()
    avgCreditScore.show()

    //Writing to csv file for average credit score
    avgCreditScore.write.csv("/Users/C81447B/Documents/Personal/Clearscore_Test/output/question1")

    avgCreditScore


  }

  def question2(inputDF: DataFrame) : DataFrame = {

    //Parse input account DF to get the user id and employmentStatus
    val parseAccountDF = inputDF
      .select("account.user.*")
      .select("id","employmentStatus")

    //Counting total number of users by employmentStatus
    val totalUsersByEmp =parseAccountDF
      .groupBy("employmentStatus")
      .count()
      .orderBy(desc("count"))

    //Writing to CSV file
    totalUsersByEmp.write.csv("/Users/C81447B/Documents/Personal/Clearscore_Test/output/question2")

    totalUsersByEmp
  }

  def question3(inputDF: DataFrame) :DataFrame = {

    //Parsing input Report DF to get report->ScoreBlock->Delphi data for the latest report.
    val latestScoreDF = inputDF
       .select("user-uuid","pulled-timestamp","report.ScoreBlock.Delphi")
       .select(col("user-uuid"),col("pulled-timestamp"),explode(col("Delphi")).alias("newDelphi"))
       .select(col("user-uuid"),col("pulled-timestamp"),col("newDelphi.*"))
       .groupBy("user-uuid")
       .agg(max("pulled-timestamp").as("pulled-timestamp"),
           first("Score").as("score"))
       .select("user-uuid","pulled-timestamp","score")

    //Creating scoreDf with range and value column where each range is incremented by 50.
    val scoreDF = latestScoreDF
      .withColumn("range",when(col("score") >=0 && col("score") <=50 ,lit("0-50"))
        .otherwise(
          when(col("score") >=450 && col("score") <=500 ,lit("450-500"))
           .otherwise(
             when (col("score") >500 && col("score") <=550 ,lit("500-550"))
              .otherwise(
                when(col("score") >550 && col("score") <=600 ,lit("550-600"))
                 .otherwise(
                    when(col("score") >600 && col("score") <=650 ,lit("600-650"))
                      .otherwise(when(col("score") >650 && col("score") <=700 ,lit("650-700"))))))))
      .withColumn("value",lit(1))

    //Calculate total number of records/users by range.
    val scoreRangeDF = scoreDF
           .groupBy("range")
           .agg(sum("value").alias("total_users"))
           .select("range","total_users")

    scoreRangeDF.show()

    //Writing to CSV file
    scoreRangeDF.write.csv("/Users/C81447B/Documents/Personal/Clearscore_Test/output/question3")

    scoreRangeDF

  }

  def question4(reportsDF : DataFrame,accountDF : DataFrame) : DataFrame = {

    val activeBankAccounts = "report.Summary.Payment_Profiles.CPA.Bank.Total_number_of_Bank_Active_accounts_"
    val outstandingBalance = "report.Summary.Payment_Profiles.CPA.Bank.Total_outstanding_balance_on_Bank_active_accounts"

    //Parse report DF to get the required attributes from latest report.
    val reportsDFLatest = reportsDF
      .select("user-uuid","pulled-timestamp",activeBankAccounts,outstandingBalance)
      .groupBy("user-uuid")
      .agg( max("pulled-timestamp").alias("latest_pulled_timestamp"),
        first("Total_number_of_Bank_Active_accounts_").alias("Total_number_of_Bank_Active_accounts_"),
        first("Total_outstanding_balance_on_Bank_active_accounts").alias("Total_outstanding_balance_on_Bank_active_accounts"))

    //Parse account DF to get the required attributes.
    val accountDFNew = accountDF
        .select("uuid","account.user.bankName","account.user.employmentStatus")

    //Join parsed report DF and account DF.
    val resultDF = reportsDFLatest.join(accountDFNew,reportsDFLatest("user-uuid") === accountDFNew("uuid"),"inner" )
      .select(
        reportsDFLatest("user-uuid"),
        accountDFNew("employmentStatus"),
        accountDFNew("bankName"),
        reportsDFLatest("Total_number_of_Bank_Active_accounts_"),
        reportsDFLatest("Total_outstanding_balance_on_Bank_active_accounts")
      )

    resultDF.show()

    //Writing to CSV file
    resultDF.write.csv("/Users/C81447B/Documents/Personal/Clearscore_Test/output/question4")

    resultDF

  }

}
