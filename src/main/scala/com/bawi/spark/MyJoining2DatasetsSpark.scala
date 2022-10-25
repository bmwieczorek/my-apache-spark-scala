package com.bawi.spark

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

case class A(id: String, name: String)
case class B(id: String, name2: String)

object MyJoining2DatasetsSpark {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val ads = sparkSession.read.csv("a.csv")
      .toDF("id", "name")
    ads.printSchema()
    val bds: DataFrame = sparkSession.read.csv("b.csv")
      .toDF("id", "name2")
    bds.printSchema()
    val joined = bds.join(ads, "id")
    joined.printSchema()
    joined.show(10, truncate = false)
    joined.repartition(1).write
      .option("emptyValue", null)
      .option("nullValue", null)
      .csv("result.csv")
  }

}
