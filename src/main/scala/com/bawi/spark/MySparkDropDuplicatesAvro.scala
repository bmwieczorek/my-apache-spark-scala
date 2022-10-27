package com.bawi.spark

import org.apache.spark.sql.SparkSession

object MySparkDropDuplicatesAvro {
  def main(args: Array[String]): Unit = {
    //         spark2-shell --jars /tmp/spark-avro_2.11-4.0.0.jar --executor-memory 3G <<< "spark.read.format(\"com.databricks.spark.avro\").load(\"$INPUT_PATH_HOUR\").dropDuplicates(\"edaMessageID\").write.format(\"com.databricks.spark.avro\").save(\"$OUTPUT_PATH_HOUR\")"
    val spark = SparkSession.builder().appName(MySparkDropDuplicatesAvro.getClass.getSimpleName).getOrCreate()
    val df = spark.read.format("com.databricks.spark.avro").load("/data/core/year=2020/month=07/day=20/hour=06/*.avro")
    df.dropDuplicates("edaMessageID")//.coalesce()
//    df.show(3)

    df.write.format("com.databricks.spark.avro").save("/data/core/deduplicate-test/year=2020/month=07/day=20/hour=06/")
  }
}
