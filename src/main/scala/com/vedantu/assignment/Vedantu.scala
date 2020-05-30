package com.vedantu.assignment

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}



object  Vedantu {

  def getSpark: SparkSession ={

    val sparkConf = new SparkConf(true)
      .setAppName("RunningTotalOrderCount")
      .set("spark.driver.maxResultSize", "2g")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "1024m")
      .set("spark.kryoserializer.buffer", "512")
      .set("spark.yarn.executor.memoryOverhead", "8192")

    SparkSession.builder().config(sparkConf.setMaster("local[4]")).enableHiveSupport().getOrCreate()
  }

  def loadData(sparkSession: SparkSession):DataFrame = {

    val df =sparkSession.read.option("multiLine", true).json(new File("UpstreamDataSources").getAbsolutePath())
    println(s"Total loaded data = ${df.count()}")
    df
  }

  def calculateAvgDuration(sparkSession: SparkSession) ={

    sparkSession.sql(
      s""" select
         | appnameenc,
         | avg(duration) avg_duration
         | from
         |(
         |  select
         |  sessionid,
         |  appnameenc,
         |  coalesce(max(timestamp)-min(timestamp),0) as duration
         |  from clic_ass
         |  where appnameenc =1 or appnameenc =2
         |  group  by sessionid ,appnameenc
         |) tmp
         | group by appnameenc
         | """.stripMargin
    )

  }

  def countUserId(sparkSession: SparkSession): DataFrame ={

    sparkSession.sql(
      s"""select
         |region,
         |count(distinct calc_userid) calc_calc_userid
         |from
         |clic_ass
         |where calc_userid is not null
         |or calc_userid != "-"
         |group by region
         |""".stripMargin
    )
  }

  def definingActions(sparkSession: SparkSession): DataFrame ={

    val aggData = sparkSession.sql(
      s"""
         |select
         |calc_userid,
         |eventlaenc,
         |min(timestamp) timestamp,
         |count(1) as action_count
         |from clic_ass
         |where eventlaenc in (126,107)
         |group by calc_userid,eventlaenc
         |""".stripMargin
    )

    aggData.createOrReplaceTempView("agg_data")

    sparkSession.sql(
      s"""
         |select
         |calc_userid,
         |max(case when rnk=1 then eventlaenc end ) as first_action,
         |max(case when rnk=1 then action_count end) as first_action_count,
         |max(case when rnk=2 then eventlaenc end) as second_action,
         |max(case when rnk=2 then action_count end) as second_action_count
         |from
         |(
           |select
           |calc_userid,
           |eventlaenc,
           |timestamp,
           |action_count,
           |row_number() over(partition by calc_userid order by timestamp) as rnk
           |from agg_data
           |) tmp
           |group by calc_userid
         |""".stripMargin
    )
  }

  def main(args: Array[String])
  {
    val spark = getSpark

    val numRows = 10000

    loadData(spark).createOrReplaceTempView("clic_ass")

    println("############Displaying avg duration#############")
    calculateAvgDuration(spark).show(numRows)
    println("###########User Count Per Region #############")
    countUserId(spark).show(numRows)
    println("########### Defining Action #############")
    definingActions(spark).show(numRows)

  }
}