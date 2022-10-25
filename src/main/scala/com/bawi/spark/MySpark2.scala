package com.bawi.spark

import java.io.File
import java.nio.file.{Files, Paths}

import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.io.Directory


object MySpark2 {
  def main(args: Array[String]): Unit = {

    val output = if (args.length > 0) args(0) else "numbers"

    val sparkConf = new SparkConf().setAppName(getClass.getName)
    if (JobUtils.isLocal) {
      sparkConf.setMaster("local[*]")
      JobUtils.deleteLocalDirectory(output)
    }

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sparkContext = sparkSession.sparkContext
    val metricsCounterAccumulator = sparkContext.longAccumulator("metricsCounter")

    sparkSession.sparkContext.addSparkListener(new SparkListener {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
        if (taskEnd.taskMetrics.outputMetrics != null) {
          println(s"+++ reason=${taskEnd.reason},stageAttemptId=${taskEnd.stageAttemptId}, stageId=${taskEnd.stageId}, taskInfo.attemptNumber=${taskEnd.taskInfo.attemptNumber}, taskInfo.executorId=${taskEnd.taskInfo.executorId}, taskInfo.taskId=${taskEnd.taskInfo.taskId}, taskInfo.host=${taskEnd.taskInfo.host}, taskInfo.duration=${taskEnd.taskInfo.duration}, taskInfo.id=${taskEnd.taskInfo.id}, taskMetrics.outputMetrics.recordsWritten=${taskEnd.taskMetrics.outputMetrics.recordsWritten}, taskMetrics.outputMetrics.bytesWritten=${taskEnd.taskMetrics.outputMetrics.bytesWritten},taskType=${taskEnd.taskType}")
        }
        if (taskEnd.taskMetrics.inputMetrics != null) {
          println(s"+++ reason=${taskEnd.reason},stageAttemptId=${taskEnd.stageAttemptId}, stageId=${taskEnd.stageId}, taskInfo.attemptNumber=${taskEnd.taskInfo.attemptNumber}, taskInfo.executorId=${taskEnd.taskInfo.executorId}, taskInfo.taskId=${taskEnd.taskInfo.taskId}, taskInfo.host=${taskEnd.taskInfo.host}, taskInfo.duration=${taskEnd.taskInfo.duration}, taskInfo.id=${taskEnd.taskInfo.id}, taskMetrics.inputMetrics.recordsRead=${taskEnd.taskMetrics.inputMetrics.recordsRead}, taskMetrics.inputMetrics.bytesRead=${taskEnd.taskMetrics.inputMetrics.bytesRead},taskType=${taskEnd.taskType}")
        }
      }
    })

    val rdd: RDD[Int] = sparkContext.parallelize(Seq(1, 2, 3))
    val rdd2: RDD[Int] = rdd.map(n => {
      println(s"processing: $n")
      n
    })
    import sparkSession.implicits._
    val df: DataFrame = rdd2.toDF("numbers")

    df.createOrReplaceTempView("myview")
    val df1 = sparkSession.sql("SELECT * FROM myview")

    val countAccumulator = sparkContext.longAccumulator("count")
    val df2 = sparkSession.createDataFrame(df1.rdd.map(r => {
      countAccumulator.add(1)
      r
    }), df1.schema)


    //df2.count()

    df2.write.json(output)
    //df.write.json(output)

//    df2.show() // re-calculates
    println("***1**** " + countAccumulator.value)
    sparkSession.close()
  }

}