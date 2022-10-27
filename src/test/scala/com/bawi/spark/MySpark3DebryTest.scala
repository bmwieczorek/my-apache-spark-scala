package com.bawi.spark

import java.io.File
import org.apache.commons.io.FileUtils
import org.apache.log4j.{ConsoleAppender, Level, Logger, PatternLayout}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfter, GivenWhenThen}

class MySpark3DebryTest extends AnyFlatSpec with BeforeAndAfter with GivenWhenThen with Matchers {

    private var spark: SparkSession = _

    case class Flight(flightNumber: Int, airline_code: String)

    before {
      val outputPathFile = new File("metastore_db")
      if (outputPathFile.exists())
        FileUtils.deleteDirectory(outputPathFile)

      Logger.getRootLogger.addAppender(new ConsoleAppender(new PatternLayout("%d{yy/MM/dd HH:mm:ss} %p %c:%L %m%n")))

      Logger.getLogger("com").setLevel(Level.DEBUG)
      Logger.getLogger("org").setLevel(Level.DEBUG)

      Logger.getLogger("org.spark_project.jetty").setLevel(Level.INFO)
      Logger.getLogger("io.netty").setLevel(Level.INFO)
      Logger.getLogger("org.apache.hadoop.conf.ConfigurationProvider").setLevel(Level.INFO)
      //Logger.getLogger("DataNucleus.Persistence").setLevel(Level.INFO)
      Logger.getLogger("DataNucleus.MetaData").setLevel(Level.INFO)
      Logger.getLogger("DataNucleus.Transaction").setLevel(Level.INFO)
      Logger.getLogger("DataNucleus.Connection").setLevel(Level.INFO)
      Logger.getLogger("DataNucleus.General").setLevel(Level.INFO)
      Logger.getLogger("DataNucleus.Persistence").setLevel(Level.INFO)
      Logger.getLogger("DataNucleus.Cache").setLevel(Level.INFO)
      Logger.getLogger("DataNucleus.ValueGeneration").setLevel(Level.INFO)
      Logger.getLogger("DataNucleus.Lifecycle").setLevel(Level.INFO)
      Logger.getLogger("DataNucleus.Query").setLevel(Level.INFO) // query without params

      Logger.getLogger("DataNucleus.Datastore.Retrieve").setLevel(Level.INFO) // execution time
      Logger.getLogger("DataNucleus.Datastore.Persist").setLevel(Level.INFO) // execution time
      Logger.getLogger("DataNucleus.Datastore.Schema").setLevel(Level.INFO) // create table

      Logger.getLogger("DataNucleus.Datastore").setLevel(Level.INFO) // properties and prepare statements
      Logger.getLogger("DataNucleus.Datastore.Native").setLevel(Level.DEBUG) // sql query with params

      Logger.getLogger("org.apache.spark.ContextCleaner").setLevel(Level.WARN)
      Logger.getLogger("org.apache.spark.util.ClosureCleaner").setLevel(Level.INFO)
      Logger.getLogger("org.apache.spark.sql.catalyst.expressions.codegen").setLevel(Level.WARN)
      Logger.getLogger("org.apache.spark.sql.catalyst.planning").setLevel(Level.WARN)
      Logger.getLogger("org.apache.spark.sql.hive.client.IsolatedClientLoader").setLevel(Level.INFO)
      Logger.getLogger("org.apache.spark.sql.execution.WholeStageCodegenExec").setLevel(Level.INFO)
      Logger.getLogger("org.apache.spark.sql.hive.client.HiveClientImpl").setLevel(Level.INFO)
      Logger.getLogger("org.apache.spark.storage.DiskBlockManager").setLevel(Level.WARN)
      Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.WARN)
      Logger.getLogger("org.apache.spark.storage.BlockManagerMaster").setLevel(Level.WARN)
      Logger.getLogger("org.apache.spark.scheduler.DAGScheduler").setLevel(Level.INFO)
      Logger.getLogger("org.apache.spark.scheduler.TaskSetManager").setLevel(Level.INFO)
      Logger.getLogger("org.apache.spark.scheduler.TaskSchedulerImpl").setLevel(Level.INFO)
      Logger.getLogger("org.apache.spark.memory").setLevel(Level.INFO)
      Logger.getLogger("org.apache.spark.storage").setLevel(Level.INFO)
      Logger.getLogger("org.apache.parquet.bytes").setLevel(Level.INFO)
      Logger.getLogger("org.apache.parquet.column").setLevel(Level.INFO)
      Logger.getLogger("org.apache.parquet.io").setLevel(Level.INFO)

      val session: SparkSession = SparkSession.builder()
        .appName("MyApacheSparkTest")
//        .config("spark.hadoop.javax.jdo.option.ConnectionURL","jdbc:derby://localhost:1527/metastore_db;create=true")
//        .config("spark.hadoop.javax.jdo.option.ConnectionDriverName","org.apache.derby.jdbc.ClientDriver")
//        .config("spark.hadoop.javax.jdo.option.ConnectionUserName","APP")
//        .config("spark.hadoop.javax.jdo.option.ConnectionPassword","mine")
        .master("local[*]")
        .enableHiveSupport()
        .getOrCreate()
      spark = session

      spark.sql("create database mydb")
      import session.implicits._
      val data = """{"airline_code":"AA","country_code":"US"}"""
      val ds: Dataset[String] = Seq(data).toDS
      val df = spark.read.json(ds)
      df.write.saveAsTable("mydb.airline_to_country")
    }

    it should "process "  in {
      Given("Given")
      MySpark3.calculate(spark)
    }

  after {
//      spark.sql("drop table mydb.airline_to_country")
//      spark.sql("drop database mydb")
      if (spark != null) {
        spark.stop()
      }
    }
  }





