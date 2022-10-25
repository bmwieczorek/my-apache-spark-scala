package com.bawi.spark

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.reflect.io.Directory


object MySpark {

  case class Pair(idint: Int, idstring: String, name: String)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache").setLevel(Level.INFO)
    Logger.getLogger("org.apache.spark").setLevel(Level.DEBUG)
    Logger.getLogger("org.apache.spark.ContextCleaner").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.util.ClosureCleaner").setLevel(Level.INFO)
    Logger.getLogger("org.apache.spark.sql.execution.WholeStageCodegenExec").setLevel(Level.INFO)
    Logger.getLogger("org.apache.spark.sql.catalyst.expressions.codegen").setLevel(Level.INFO)
    Logger.getLogger("org.apache.spark.scheduler").setLevel(Level.INFO)
    Logger.getLogger("org.apache.spark.storage").setLevel(Level.INFO)
    Logger.getLogger("org.apache.spark.broadcast").setLevel(Level.INFO)
    Logger.getLogger("org.apache.spark.sql.internal").setLevel(Level.INFO)
    Logger.getLogger("org.apache.parquet").setLevel(Level.DEBUG)
    Logger.getLogger("org.apache.parquet.format.converter").setLevel(Level.INFO)
    Logger.getLogger("org.apache.parquet.io.MessageColumnIO").setLevel(Level.INFO)
    Logger.getLogger("com.bawi").setLevel(Level.INFO)

    val spark = SparkSession.builder().appName("MySpark").master("local[1]")
      .config("spark.executor.heartbeatInterval", 5 * 60 * 1000)
      .config("spark.network.timeout", 5 * 60 * 1000 + 1)
      .config("parquet.strings.signed-min-max.enabled", true)
      .getOrCreate()

    new Directory(new File("/tmp/spark-idea")).deleteRecursively()
    import spark.implicits._
//    val pairs: Seq[Pair] = scala.util.Random.shuffle((1 to 1000).toList).map(i => Pair(i, String.valueOf(i)))
//    val pairs: Seq[Pair] = scala.util.Random.shuffle(Seq(1, 2)).map(i => Pair(i, String.valueOf(i)))
//    val pairs = Seq((11, "cd"), (22, ""), (33, " "), (44, null), (55, "a")).map(t => Pair(t._1, String.valueOf(t._1), t._2))
    val pairs: Seq[Pair] = Seq((11, "cd"), (22, ""), (33, " "), (44, null), (55, "a")).map(t => Pair(t._1, String.valueOf(t._1), t._2))
//    spark.sparkContext.parallelize(pairs, 1)
    pairs
      .toDF().write.parquet("/tmp/spark-idea")

    Logger.getLogger("MySpark").info("****READING****")

    spark.read.parquet("/tmp/spark-idea")
      .filter("idint = 33")
      .show()
  }
}
