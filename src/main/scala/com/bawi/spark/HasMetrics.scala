package com.bawi.spark

import org.apache.spark.groupon.metrics.UserMetricsSystem

trait HasMetrics {

  def getMetricsListener: CustomSparkMetricsListener = {
    new CustomSparkMetricsListener {
      override def onMetric(str: String, value: Long): Unit = {
        UserMetricsSystem.counter(str).inc(value)
      }
    }
  }

}
