package com.bawi.spark.my.java3;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

interface SparkBase {
    Logger LOGGER = LoggerFactory.getLogger(SparkBase.class);

    default void setupMetrics(SparkSession sparkSession) {}
    void runSpark(SparkSession sparkSession);

    default void start() {
        SparkSession sparkSession = SparkSession.builder()
                .appName(getClass().getSimpleName())
                .master("local[*]")
                .getOrCreate();

        setupMetrics(sparkSession);

        runSpark(sparkSession);

        sparkSession.stop();
    }

}
