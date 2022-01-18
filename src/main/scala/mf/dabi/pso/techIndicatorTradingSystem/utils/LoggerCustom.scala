package mf.dabi.pso.techIndicatorTradingSystem.utils

import mf.dabi.pso.techIndicatorTradingSystem.settings.Sparkable
import org.apache.spark.sql.DataFrame
import org.slf4j.Logger

object LoggerCustom extends Sparkable {

  def loggVolumetria(df: DataFrame, sep: String = "*"): Unit =
    loggerPrettyPrinter(logger, 3, sep, s"Count: ${df.count()}", s"DistinctCount: ${df.distinct().count()}")

  def loggerPrettyHeader(logger: Logger, mssg: String, hj: Int, sep: String = "#"): Unit = loggerPrettyPrinter(logger, hj, sep, mssg)

  def loggerPrettyPrinter(logger: Logger, hj: Int, sep: String, mssgs: String*): Unit = {
    val long: Int = mssgs.map(_.length).max + 8
    val index: Int = if (hj >= 4) 0 else 4 - hj
    val header: Array[String] = if (index == 0) Array() else (0 until index).map(_ => s"${sep * long}").toArray
    val allLog: Array[String] = header ++ mssgs.map(mssg => s"${sep * 2} $mssg") ++ header
    allLog foreach logger.warn
  }
}
