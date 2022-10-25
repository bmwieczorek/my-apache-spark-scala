package com.bawi.spark

import java.net.InetAddress
import java.time.Instant

import org.apache.spark.scheduler._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.{ExceptionFailure, SparkContext, TaskFailedReason}



class CustomSparkMetrics(listener: CustomSparkMetricsListener) {

  def registerSparkListener(sc: SparkContext): Unit = {
    sc.addSparkListener(new SparkListener {

      private val applicationStartTime = Instant.now().toEpochMilli

      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
        if (listener != null && taskEnd != null && taskEnd.taskMetrics != null) {
          taskEnd.reason match {
            case org.apache.spark.Success =>
              listener.onMetric(s"""TaskStatus_Success.${appendTags(taskEnd)}""", 1)
            //println(s"""TaskStatus_Success.${appendTags(taskEnd)}""")
            case e: ExceptionFailure =>
              listener.onMetric(s"""TaskStatus_ExceptionFailure.${appendTags(taskEnd)}""", 1)
              listener.onMetric(s"""TaskStatus_ExceptionFailureMsg.${substr(e.exception.get.getCause.getMessage)}""", 1)
              println(s"""TaskStatus_ExceptionFailureMsg.${appendTags(taskEnd)}.${e.exception.get.getCause.getMessage}""")
            case e: TaskFailedReason =>
              listener.onMetric(s"""TaskStatus_TaskFailedReason.${appendTags(taskEnd)}""", 1)
              listener.onMetric(s"""TaskStatus_TaskFailedReasonMsg.${substr(e.toErrorString)}""", 1)
              println(s"""TaskStatus_TaskFailedReasonMsg.${appendTags(taskEnd)}.${e.toErrorString}""")
          }
        }

        listener.onMetric(s"""Task_executorDeserialTime.${appendTags(taskEnd)}""", taskEnd.taskMetrics.executorDeserializeTime)
        listener.onMetric(s"""Task_executorDeserialCpuTime.${appendTags(taskEnd)}""", taskEnd.taskMetrics.executorDeserializeCpuTime)
        listener.onMetric(s"""Task_executorRunTime.${appendTags(taskEnd)}""", taskEnd.taskMetrics.executorRunTime)
        listener.onMetric(s"""Task_executorCpuTime.${appendTags(taskEnd)}""", taskEnd.taskMetrics.executorCpuTime)
        listener.onMetric(s"""Task_resultSize.${appendTags(taskEnd)}""", taskEnd.taskMetrics.resultSize)
        listener.onMetric(s"""Task_jvmGCTime.${appendTags(taskEnd)}""", taskEnd.taskMetrics.jvmGCTime)
        listener.onMetric(s"""Task_resultSerializationTime.${appendTags(taskEnd)}""", taskEnd.taskMetrics.resultSerializationTime)
        listener.onMetric(s"""Task_memoryBytesSpilled.${appendTags(taskEnd)}""", taskEnd.taskMetrics.memoryBytesSpilled)
        listener.onMetric(s"""Task_diskBytesSpilled.${appendTags(taskEnd)}""", taskEnd.taskMetrics.diskBytesSpilled)
        listener.onMetric(s"""Task_peakExecutionMemory.${appendTags(taskEnd)}""", taskEnd.taskMetrics.peakExecutionMemory)

        listener.onMetric(s"""Task_shufW_bytesWritten.${appendTags(taskEnd)}""", taskEnd.taskMetrics.shuffleWriteMetrics.bytesWritten)
        listener.onMetric(s"""Task_shufW_recordsWritten.${appendTags(taskEnd)}""", taskEnd.taskMetrics.shuffleWriteMetrics.recordsWritten)
        listener.onMetric(s"""Task_shufW_writeTime.${appendTags(taskEnd)}""", taskEnd.taskMetrics.shuffleWriteMetrics.writeTime)

        listener.onMetric(s"""Task_shufR_remoteBlocksFetched.${appendTags(taskEnd)}""", taskEnd.taskMetrics.shuffleReadMetrics.remoteBlocksFetched)
        listener.onMetric(s"""Task_shufR_localBlocksFetched.${appendTags(taskEnd)}""", taskEnd.taskMetrics.shuffleReadMetrics.localBlocksFetched)
        listener.onMetric(s"""Task_shufR_remoteBytesRead.${appendTags(taskEnd)}""", taskEnd.taskMetrics.shuffleReadMetrics.remoteBytesRead)
        listener.onMetric(s"""Task_shufR_localBytesRead.${appendTags(taskEnd)}""", taskEnd.taskMetrics.shuffleReadMetrics.localBytesRead)
        listener.onMetric(s"""Task_shufR_fetchWaitTime.${appendTags(taskEnd)}""", taskEnd.taskMetrics.shuffleReadMetrics.fetchWaitTime)
        listener.onMetric(s"""Task_shufR_recordsRead.${appendTags(taskEnd)}""", taskEnd.taskMetrics.shuffleReadMetrics.recordsRead)
        listener.onMetric(s"""Task_shufR_totalBytesRead.${appendTags(taskEnd)}""", taskEnd.taskMetrics.shuffleReadMetrics.totalBytesRead)
        listener.onMetric(s"""Task_shufR_totalBlocksFetched.${appendTags(taskEnd)}""", taskEnd.taskMetrics.shuffleReadMetrics.totalBlocksFetched)

        listener.onMetric(s"""Task_output_bytesWritten.${appendTags(taskEnd)}""", taskEnd.taskMetrics.outputMetrics.bytesWritten)
        listener.onMetric(s"""Task_output_recordsWritten.${appendTags(taskEnd)}""", taskEnd.taskMetrics.outputMetrics.recordsWritten)

        listener.onMetric(s"""Task_input_recordsRead.${appendTags(taskEnd)}""", taskEnd.taskMetrics.inputMetrics.recordsRead)
        listener.onMetric(s"""Task_input_bytesRead.${appendTags(taskEnd)}""", taskEnd.taskMetrics.inputMetrics.bytesRead)
      }

      override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
        val applicationEndTime = Option(applicationEnd).map(_.time).getOrElse(0L)
        val durationMillis = applicationEndTime - applicationStartTime
        listener.onMetric(s"""applicationElapsedTimeInSecs.$resolveHostName""", durationMillis/1000)
      }
    })
  }

  def registerQueryExecutionListener(spark: SparkSession): Unit = {
    spark.listenerManager.register(new QueryExecutionListener {
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
        listener.onMetric(s"""QueryFailure_$funcName.${InetAddress.getLocalHost.getHostName.split("\\.")(0)}""", 1)
        listener.onMetric(s"""QueryFailureMsg_$funcName.${substr(exception.getCause.getMessage)}""", 1)
        println(s"""QueryFailureMsg_$funcName.${InetAddress.getLocalHost.getHostName.split("\\.")(0)}.${exception.getCause.getMessage}""")
      }

      override def onSuccess(funcName: String, qe: QueryExecution, durationInNanos: Long): Unit = {
        listener.onMetric(s"""QuerySuccessDuration_$funcName.${InetAddress.getLocalHost.getHostName.split("\\.")(0)}""", durationInNanos/1000000000L)
        println(s"""QuerySuccess_Duration_$funcName.$resolveHostName""", durationInNanos / 1000000000.0)
      }
    })
  }

  private def appendTags(taskEnd: SparkListenerTaskEnd): String = {
    val info = taskEnd.taskInfo
    s"""${substr(info.host.split("\\.")(0))}"""
  }

  private def substr(string: String): String = {
    if (string == null) "" else string.substring(0, Math.min(string.length, 30)).replaceAll("\\.","_")
  }

  private def resolveHostName = InetAddress.getLocalHost.getHostName.split("\\.")(0)

}
