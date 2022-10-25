package com.bawi.spark

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.reflect.io.Directory


object MySpark3 {

  case class Flight(flightNumber: Int, airline_code: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("MySpark3").master("local[*]").getOrCreate()
    calculate(spark)
    spark.close()
  }

  def calculate(spark: SparkSession) = {
    import spark.sqlContext.implicits._
    val flightsDF = Seq(Flight(1234, "AA")).toDF
    flightsDF.createOrReplaceTempView("flightsView")
    val airlineToCountry = spark.sql("SELECT * FROM mydb.airline_to_country")
    airlineToCountry.createOrReplaceTempView("airlineToCountryView")
    val resultDF = spark.sql(
      """SELECT f.*, a2c.country_code
           FROM flightsView f
           JOIN airlineToCountryView a2c
           ON f.airline_code = a2c.airline_code""")
    resultDF.show
  }
}
