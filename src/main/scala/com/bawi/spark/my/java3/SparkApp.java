package com.bawi.spark.my.java3;

public class SparkApp implements SparkBase, Read, Write, LoggingInfoListenerRegistrar, CustomSparkMetricsRegistrar, ConfigurationProvider {

    public static void main(String[] args) {
        new SparkApp().start();
    }

    @Override
    public Configuration getConfiguration() {
        return new Configuration("my.properties");
    }
}
