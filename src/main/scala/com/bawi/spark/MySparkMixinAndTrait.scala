package com.bawi.spark
/*
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkContext
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.LongAccumulator
import org.slf4j.{Logger, LoggerFactory}


class ConfigurationProvider(configName: String) extends Serializable {
  private val LOGGER: Logger = LoggerFactory.getLogger(getClass)
  lazy val config: Config = ConfigFactory.load(configName)

  def logDebug(): Unit = {
    LOGGER.debug("Config: {}", config.entrySet().toArray.mkString(","))
  }

  def getString(path: String): String = {
    config.getString(path)
  }
}

class VerticaConfiguration(configName: String) extends ConfigurationProvider(configName) {

  def verticaHost: String = {
    config.getString("vertica.host")
  }

  def verticaDatabase: String = {
    config.getString("vertica.database")
  }

  def verticaDbSchema: String = {
    config.getString("vertica.dbSchema")
  }

  def verticaTableName: String = {
    config.getString("vertica.tableName")
  }

  def verticaUser: String = {
    config.getString("vertica.user")
  }

  def verticaPassword: String = {
    config.getString("vertica.password")
  }

  def verticaSaveMode: String = {
    config.getString("vertica.saveMode")
  }

  def verticaTableTruncateOnOverwrite: String = {
    config.getString("vertica.tableTruncateOnOverwrite")
  }

  def verticaConnectionLoadBalance: String = {
    config.getString("vertica.connectionLoadBalance")
  }

  def verticaLoginTimeout: Int = {
    config.getInt("vertica.loginTimeout")
  }
}

class AppConfiguration(configName: String) extends VerticaConfiguration(configName: String)

trait ConfigurationProvider[A <: ConfigurationProvider] {
  def configuration: A
}

trait ApplicationBase extends SparkMetrics {

  def runSparkApplication(spark: SparkSession): Unit

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    setUpMetrics(spark)

    runSparkApplication(spark)

    spark.stop()
  }

  private def setUpMetrics(spark: SparkSession): Unit = {
    val sc = spark.sparkContext
    UserMetricsSystem.initialize(sc, "custom_metrics")
    val customSparkMetrics = new CustomSparkMetrics(getMetricsListener)
    customSparkMetrics.registerSparkListener(sc)

    sc.addSparkListener(new SparkListener {
      private val applicationStartTime = Instant.now().toEpochMilli

      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
        if (taskEnd != null && taskEnd.taskMetrics != null) {
          UserMetricsSystem.gauge(s"""Task_app_elapsed_time_seconds.${appendTags(taskEnd)}""").set((Instant.now().toEpochMilli - applicationStartTime) / 1000)
        }
      }
    })
    customSparkMetrics.registerQueryExecutionListener(spark)
  }

  def appendTags(taskEnd: SparkListenerTaskEnd): String = {
    val info = taskEnd.taskInfo
    s"""${substr(info.host.split("\\.")(0))}_e${info.executorId}s${taskEnd.stageId}t${info.taskId}a${info.attemptNumber}"""
  }

  def substr(string: String): String = {
    if (string == null) "" else string.substring(0, Math.min(string.length, 30)).replaceAll("\\.", "_")
  }
}

case class Metrics(recordsWrittenCountAcc: LongAccumulator)

trait HiveRead[A <: ConfigurationProvider] extends Read with ConfigurationProvider[A] {
  def read(sparkSession: SparkSession, metrics: Metrics): DataFrame = {
    sparkSession.sqlContext.sql("""""")
  }
}

trait VerticaWrite[A <: VerticaConfiguration] extends Write with ConfigurationProvider[A] {
  def write(df: DataFrame): Unit = {
    val dialect = new VerticaDialect
    JdbcDialects.registerDialect(dialect)
    df.write.format("jdbc")
      .option("url", s"jdbc:vertica://${configuration.verticaHost}:5433/${configuration.verticaDatabase}")
      .option("driver", "com.vertica.jdbc.Driver")
      .option("user", configuration.verticaUser)
      .option("password", configuration.verticaPassword)
      .option("dbtable", s"${configuration.verticaDbSchema}.${configuration.verticaTableName}")
      .mode(configuration.verticaSaveMode)
      .option("truncate", configuration.verticaTableTruncateOnOverwrite)
      .option("loginTimeout", configuration.verticaLoginTimeout)
      .option("ConnectionLoadBalance", configuration.verticaConnectionLoadBalance)
      .save()
  }
}

trait Read {
  def read(sparkSession: SparkSession, metrics: Metrics): DataFrame
}

trait Write {
  def write(df: DataFrame)
}

abstract class IngestApp(sparkSession: SparkSession) extends Read with Write {
  private val LOGGER = LoggerFactory.getLogger(getClass)

  def start(): Unit = {
    val startTimeMillis = System.currentTimeMillis()
    val sparkContext: SparkContext = sparkSession.sparkContext
    val recordsWrittenCountAcc: LongAccumulator = sparkContext.longAccumulator("recordsWrittenCountAcc")
    val metrics = Metrics(recordsWrittenCountAcc)
    try {
      val df = read(sparkSession, metrics)
      write(df)
      logAndPublish(startTimeMillis, metrics, "SUCCESS")
    } catch {
      case e: Exception =>
        logAndPublish(startTimeMillis, metrics, e.getCause.getMessage)
        throw e
    }
  }

  private def logAndPublish(startTimeMs: Long, metrics: Metrics, message: String): Unit = {
    val recordsWrittenCount = metrics.recordsWrittenCountAcc.value
    val endTimeMs = System.currentTimeMillis()
    val elapsedTimeMinutes = (endTimeMs - startTimeMs).toDouble / 60000

    UserMetricsSystem.gauge(s"elapsedTimeMinutes.$message").set(elapsedTimeMinutes)
    UserMetricsSystem.gauge(substr(s"recordsWrittenCount.$message")).set(recordsWrittenCount.longValue())
    UserMetricsSystem.counter(substr(s"runStsMgs.$message")).inc()

    LOGGER.info(s"Driver ($resolveHostName): " +
      s"runStsMsg: $message, " +
      s"recordsWrittenCount=${recordsWrittenCount.longValue()}, " +
      s"elapsedTimeMinutes=$elapsedTimeMinutes")
      metrics.recordsWrittenCountAcc.reset()
  }

  private def resolveHostName = InetAddress.getLocalHost.getHostName.split("\\.")(0)

  private def substr(string: String): String = {
    if (string == null) "" else string.substring(0, Math.min(string.length, 30)).replaceAll("\\.", "_")
  }
}

object MySparkMixinAndTrait extends ApplicationBase {
  override def runSparkApplication(sparkSession: SparkSession): Unit = {
    class IngestAppImpl extends IngestApp(sparkSession) with HiveRead[AppConfiguration] with VerticaWrite[AppConfiguration] {
      override def configuration: AppConfiguration = new AppConfiguration("app.properties")
    }
    new IngestAppImpl().start()
  }
}
*/
