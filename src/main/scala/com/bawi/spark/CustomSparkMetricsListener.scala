package com.bawi.spark

trait CustomSparkMetricsListener {
  def onMetric(str: String, value: Long): Unit = {
  }
}
