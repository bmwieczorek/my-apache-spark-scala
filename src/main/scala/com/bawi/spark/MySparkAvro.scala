package com.bawi.spark

import org.apache.spark.sql.SparkSession

object MySparkAvro {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val df = spark.read.format("avro").load("/tmp/flume/160266703*")
    df.filter("agencyEnrichment = true").show(3)

  }
}
