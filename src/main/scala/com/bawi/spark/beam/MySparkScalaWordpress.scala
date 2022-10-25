package com.bawi.spark.beam

import java.util.Random

import com.bawi.spark.beam.InMemoryReadAndConsoleOutScala.getClass
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object MySparkScalaWordpress {

  case class Person(name: String, age: Integer)

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName(getClass.getSimpleName).getOrCreate()

    val stringRDD: RDD[String] = sparkSession.sparkContext.parallelize(Seq("a", "bb", "ccc"))

    val personRDD: RDD[Person] = stringRDD.map(string => Person(string, new java.util.Random().nextInt(100)))

    import sparkSession.implicits._
    val personDF: DataFrame = personRDD.toDF()
    personDF.show()
  }
}
