package com.bawi.spark.beam

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, RowFactory, SparkSession}

object InMemoryReadAndConsoleOutScala {
  case class Person(name: String, age: Integer)

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName(getClass.getSimpleName).getOrCreate()

    val stringRDD: RDD[String] = sparkSession.sparkContext.parallelize(Seq("a", "bb", "ccc"))

    val personRDD: RDD[Person] = stringRDD.map(string => Person(string, new java.util.Random().nextInt(100)))
//    personRDD.foreach(s => println(s))
//    personRDD.foreach(println) // same as above

    import sparkSession.implicits._
    val personDF: DataFrame = personRDD.toDF()
//    personDF.printSchema()
//    personDF.show()

    val personDf: DataFrame = Seq(Person("Bob", 21), Person("Alice", 20)).toDF()
    val ds_2: Dataset[Person] = personDF.as[Person]
    val personDs: Dataset[Person] = Seq(Person("Bob", 21), Person("Alice", 20)).toDS()
    val df_2 = personDs.toDF()
    df_2.show()

val rowRDD: RDD[Row] = stringRDD.map(string => RowFactory.create(string))
val rowRDD2: RDD[Row] = stringRDD.map(RowFactory.create(_))




    val rdd: RDD[String] = sparkSession.sparkContext.parallelize(Seq("a", "bb", "ccc", "ddd"))
    val value: RDD[Row] = rdd.map(string => RowFactory.create(string))
    rdd.foreach(s => println(s))
    rdd.foreach(println) // same as above

    import sparkSession.implicits._
    // .toDF/.toDS requires sparkSession.implicits._
    val df: DataFrame = rdd.toDF()

    df.printSchema() // root |-- value: string (nullable = true)
    df.show() // column value

    val df2: DataFrame = rdd.toDF("str_name") // explicitly set column name
    df2.filter("length(str_name) > 1").show()
    df2.filter(r => r.getAs[String]("str_name").length > 1)
    df2.printSchema() // root |-- name: string (nullable = true)
    df2.show()
    df2.foreach(r => println(r.get(0) + ", " + r.getAs[String]("str_name") + ", " + r.mkString + ", " + r.schema))
    // a, a, a, StructType(StructField(str_name,StringType,true))

    val ds: Dataset[String] = rdd.toDS()
    ds.filter(s => s.length > 0)
    ds.show() // with value column name
    ds.printSchema()
    val rdd1: RDD[String] = ds.rdd
    val schema1: StructType = ds.schema
    println("schema1:" + schema1) // schema1:StructType(StructField(value,StringType,true))

    val df3: DataFrame = ds.toDF("other_name")
    val rdd3: RDD[Row] = df3.rdd
    val schema3: StructType = df3.schema
    println("schema3:" + schema3) // StructType(StructField(other_name,StringType,true))

    val ds2: Dataset[String] = df3.as[String]
    val strings: Array[String] = ds2.collect()
    println(strings.mkString(",")) // a,bb,ccc,ddd

    // .toDF/.toDS requires sparkSession.implicits._
    val df11: DataFrame = Seq("X", "Y", "Z").toDF()
    val df12: DataFrame = Seq("X", "Y", "Z").toDF("my_string")
    val ds11: Dataset[String] = Seq("X", "Y", "Z").toDS()

    val ds21: Dataset[String] = sparkSession.createDataset[String](rdd)
    val ds22: Dataset[String] = sparkSession.createDataset(Seq("X", "Y", "Z"))
    val ds23: Dataset[String] = sparkSession.createDataset(util.Arrays.asList("X", "Y"))

    val df31: DataFrame = sparkSession.createDataFrame(rowRDD, StructType(Seq(StructField("my_column_name", StringType))))
    val ds31: Dataset[Person] = sparkSession.createDataset(util.Arrays.asList(Person("Bob", 21)))


//    val df31 = sparkSession.createDataFrame(rdd);
//    df31.printSchema()
//    df31.show()

  }
}
