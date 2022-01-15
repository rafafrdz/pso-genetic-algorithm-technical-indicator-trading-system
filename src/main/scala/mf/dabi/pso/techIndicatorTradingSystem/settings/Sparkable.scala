package mf.dabi.pso.techIndicatorTradingSystem.settings

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

trait Sparkable extends Serializable {

  lazy val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()


  lazy val sc: SparkContext = spark.sparkContext


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
