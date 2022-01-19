package mf.dabi.pso.techIndicatorTradingSystem.settings

import org.apache.log4j.{Level, Logger => sL}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

trait Sparkable extends Serializable {
  self =>

  lazy val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

  spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

  lazy val sc: SparkContext = spark.sparkContext

  @transient implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  //  sL.getLogger(self.getClass).setLevel(Level.WARN)
  setLog(Level.WARN)

  def setLog(level: Level): Unit = {
    sL.getLogger("org.apache.spark").setLevel(level)
    sc.setLogLevel(level.toString)
    val rootLogger: sL = sL.getRootLogger
    rootLogger.setLevel(level)
  }


  sealed trait SparkIO[T] {
    def csv(path: String, header: Boolean, sep: String): T

    def parquet(path: String): T
  }

  case object Read extends SparkIO[DataFrame] {
    def csv(path: String, header: Boolean = true, sep: String = ","): DataFrame =
      spark.read.option("header", header).option("sep", sep).csv(path)

    def parquet(path: String): DataFrame = spark.read.parquet(path)
  }

  case class Write(df: DataFrame, saveMode: SaveMode = SaveMode.Overwrite) extends SparkIO[Unit] {
    def csv(path: String, header: Boolean = true, sep: String = ","): Unit =
      df.write.option("header", header).option("sep", sep).csv(path)

    def parquet(path: String): Unit = df.write.mode(saveMode).parquet(path)
  }


}
