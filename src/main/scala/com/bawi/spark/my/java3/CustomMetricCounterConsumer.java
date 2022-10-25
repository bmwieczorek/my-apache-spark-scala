package com.bawi.spark.my.java3;

@FunctionalInterface
public interface CustomMetricCounterConsumer {
    void onMetric(String key, long value);
}