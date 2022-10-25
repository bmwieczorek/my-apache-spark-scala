package com.bawi.spark
import org.apache.spark.sql.SparkSession

import scala.collection.immutable


object SparkRepartition {

  case class CountryCodeAndIndex(code: String, index: Int)

  def main(args: Array[String]): Unit = {

    val dataset: immutable.Seq[CountryCodeAndIndex] =
              (30 to 50).map(CountryCodeAndIndex("PL", _))
        .union((1 to 20).map(CountryCodeAndIndex("DE", _)))
        .union((1 to 30).map(CountryCodeAndIndex("PL", _)))
        .union((1 to 15).map(CountryCodeAndIndex("SK", _)))
        .union((1 to 10).map(CountryCodeAndIndex("FR", _)))
        .union((20 to 30).map(CountryCodeAndIndex("DE", _)))

    val spark = SparkSession.builder().master("local").getOrCreate()


    import spark.implicits._
    import org.apache.spark.sql.functions._

    val df = dataset.toDF.repartitionByRange(2, col("code"), col("index"))
//    val df = dataset.toDF.repartition( col("code"), col("index")) // many files (each file one or more rows) not recommended
//    val df = dataset.toDF.repartition( col("code")) // separate file per code (different file sizes)
    df.explain()
    df.write.csv("country_code_index")
  }
}
