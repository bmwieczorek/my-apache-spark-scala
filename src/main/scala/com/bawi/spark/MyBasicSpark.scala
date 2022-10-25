package com.bawi.spark


import org.apache.log4j.{ConsoleAppender, Level, Logger, PatternLayout}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.immutable


object MyBasicSpark {

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.addAppender(new ConsoleAppender(new PatternLayout("%d{yy/MM/dd HH:mm:ss} %p %c:%l %m%n")))

    Logger.getLogger("com").setLevel(Level.DEBUG)
    Logger.getLogger("org").setLevel(Level.DEBUG)

    Logger.getLogger("org.spark_project.jetty").setLevel(Level.INFO)
    Logger.getLogger("io.netty").setLevel(Level.INFO)
    Logger.getLogger("org.apache.spark.ContextCleaner").setLevel(Level.WARN)/*
    Logger.getLogger("org.apache.spark.util.ClosureCleaner").setLevel(Level.INFO)
    Logger.getLogger("org.apache.spark.sql.execution.WholeStageCodegenExec").setLevel(Level.INFO)
    Logger.getLogger("org.apache.spark.sql.catalyst.expressions.codegen").setLevel(Level.WARN)*/
    //    Logger.getLogger("org.apache.spark.sql.catalyst.planning").setLevel(Level.WARN)
    //    Logger.getLogger("org.apache.spark.sql.hive.client.IsolatedClientLoader").setLevel(Level.INFO)
    //    Logger.getLogger("org.apache.spark.sql.hive.client.HiveClientImpl").setLevel(Level.INFO)
    //    Logger.getLogger("org.apache.spark.storage.DiskBlockManager").setLevel(Level.WARN)
    //    Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.WARN)
    //    Logger.getLogger("org.apache.spark.storage.BlockManagerMaster").setLevel(Level.WARN)
    //    Logger.getLogger("org.apache.spark.scheduler.DAGScheduler").setLevel(Level.INFO)
    //    Logger.getLogger("org.apache.spark.scheduler.TaskSetManager").setLevel(Level.INFO)
    //    Logger.getLogger("org.apache.spark.scheduler.TaskSchedulerImpl").setLevel(Level.INFO)
    //    Logger.getLogger("org.apache.spark.memory").setLevel(Level.INFO)
    //    Logger.getLogger("org.apache.spark.storage").setLevel(Level.INFO)
    //    Logger.getLogger("org.apache.parquet.bytes").setLevel(Level.INFO)
    //    Logger.getLogger("org.apache.parquet.column").setLevel(Level.INFO)
    //    Logger.getLogger("org.apache.parquet.io").setLevel(Level.INFO)

    val output = "output"
    val sparkConf = new SparkConf()
    if (JobUtils.isLocal) {
      sparkConf.setMaster("local[*]")
      sparkConf.set("spark.network.timeout", "3000")
      sparkConf.set("spark.executor.heartbeatInterval", "2999")
      JobUtils.deleteLocalDirectory(output)
    }

    val spark: SparkSession = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    println(s"spark conf=${spark.sparkContext.getConf.getAll.mkString(",")}")

//    import spark.implicits.localSeqToDatasetHolder
//    import spark.implicits.newIntEncoder
    val seq: immutable.Seq[Int] = 1 to 2

    import spark.implicits._
    val df: Dataset[Row] = seq.toDF("number")
    df.write.json("output")

    spark.stop()


  }

}



