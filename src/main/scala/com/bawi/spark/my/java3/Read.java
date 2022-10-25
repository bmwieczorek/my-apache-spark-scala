package com.bawi.spark.my.java3;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;

interface Read extends SparkIngestionBase, ConfigurationProvider {
     Logger LOGGER = LoggerFactory.getLogger(Read.class);

    default @Override
    Dataset<Row> read(SparkSession sparkSession) {
        String readPath = getConfiguration().getString("read.path");
        LOGGER.info("Reading dataset from {}", readPath);
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
        JavaRDD<String> javaRDDString = javaSparkContext.parallelize(Arrays.asList("bob", "alice"));
//        Dataset<Row> ds = sparkSession.read().json(javaRDD);
// OR
        JavaRDD<Row> javaRDDRow = javaRDDString.map(RowFactory::create);
        StructField structField = DataTypes.createStructField("name", DataTypes.StringType, true);
        StructType structType = DataTypes.createStructType(Collections.singletonList(structField));
        Dataset<Row> ds = sparkSession.createDataFrame(javaRDDRow, structType);

        return ds;
    }
}
