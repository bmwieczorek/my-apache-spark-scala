package com.bawi.spark.beam

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, KeyValueGroupedDataset, SparkSession}

object MyGroupingScala {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()

    val rddString: RDD[String] = sparkSession.sparkContext.parallelize(Seq("abc", "ab", "x", "y", "abc", "y", "y"))
    val rddTuples: RDD[(String, Int)] = rddString.map(word => (word, 1))
    val reducedRddTuples: RDD[(String, Int)] = rddTuples.reduceByKey((i, j) => i + j)
    reducedRddTuples.foreach(println)

//      (ab,1)
//      (x,1)
//      (abc,2)
//      (y,3)

    import sparkSession.implicits._
    val ds: Dataset[String] = Seq("abc", "ab", "x", "y", "abc", "y", "y").toDS
    val dsTuples: Dataset[(String, Int)] = ds.map(word => (word, 1))
    dsTuples.show()

    //    +---+---+
    //    | _1| _2|
    //    +---+---+
    //    |abc|  1|
    //    | ab|  1|
    //    |  x|  1|
    //    |  y|  1|
    //    |abc|  1|
    //    |  y|  1|
    //    |  y|  1|
    //    +---+---+

    val frame3: DataFrame = dsTuples.groupBy("_1").count()
    frame3.show()

    //    +---+-----+
    //    | _1|count|
    //    +---+-----+
    //    |  x|    1|
    //    | ab|    1|
    //    |  y|    3|
    //    |abc|    2|
    //    +---+-----+


    val frame: DataFrame = dsTuples.withColumnRenamed("_1", "word").withColumnRenamed("_2", "count")
    val frame1 = frame.groupBy("word").sum("count")
    frame1.show()

//    +----+-----+
//    +----+----------+
//    |word|sum(count)|
//    +----+----------+
//    |   x|         1|
//    |  ab|         1|
//    |   y|         3|
//    | abc|         2|
//    +----+----------+

    import org.apache.spark.sql.functions._
    val frame2: DataFrame = dsTuples.groupBy("_1").agg(count("_2").as("count"))
    frame2.show()

//    +---+-----+
//    | _1|count|
//    +---+-----+
//    |  x|    1|
//    | ab|    1|
//    |  y|    3|
//    |abc|    2|
//    +---+-----+



  }

}
