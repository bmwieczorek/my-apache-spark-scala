package com.bawi.spark

package com.bawi.spark

import java.io.File
import java.nio.file.{Files, Paths}

import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object TestRecalculation {
  def main(args: Array[String]): Unit = {

    val output = args(0)
    val sparkConf = new SparkConf().setAppName(getClass.getName)
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sparkContext = sparkSession.sparkContext
    val rdd: RDD[Int] = sparkContext.parallelize(Seq(1, 2, 3))
    val rdd2: RDD[Int] = rdd.map(n => {
      println(s"processing: $n")
      n
    })
    import sparkSession.implicits._
    val df: DataFrame = rdd2.toDF("numbers")
    df.write.json(output)
    df.show() // re-calculates
    sparkSession.close()
  }

  def isMac(): Boolean = {
    if (System.getProperty("os.name").contains("Mac")) true else false
  }

  def isUbuntu(): Boolean = {
    if (System.getProperty("os.name").contains("Linux")){
      val path = Paths.get ("/etc/os-release")
      if (path.toFile.exists()) {
        new String(Files.readAllBytes(path)).toLowerCase.contains("Ubuntu")
      } else false
    } else false
  }

  def isLocal(): Boolean = {
    isMac() || isUbuntu()
  }
}
