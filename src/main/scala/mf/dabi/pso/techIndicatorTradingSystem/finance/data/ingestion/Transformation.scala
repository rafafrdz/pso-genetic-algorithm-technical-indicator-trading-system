package mf.dabi.pso.techIndicatorTradingSystem.finance.data.ingestion

import mf.dabi.pso.techIndicatorTradingSystem.finance.data.adt.AnalyzedFinanceObject
import mf.dabi.pso.techIndicatorTradingSystem.finance.data.adt.FinanceObject.finance
import mf.dabi.pso.techIndicatorTradingSystem.finance.indicators.Signal.SignalConf
import mf.dabi.pso.techIndicatorTradingSystem.finance.indicators.{Signal, SignalIndicator}
import mf.dabi.pso.techIndicatorTradingSystem.settings.Sparkable
import mf.dabi.pso.techIndicatorTradingSystem.utils.FileSystemSuite.fileName
import org.apache.spark.sql.DataFrame

trait Transformation extends Ingestion {
  self: Sparkable =>

  val dataPath: String = fsPath("data")
  val experimentPath: String = fsPath("experiment")

  final def getFinancialSignal[T <: AnalyzedFinanceObject](ticket: String, signals: SignalIndicator*): T = {
    val parquet: DataFrame = Read.parquet(s"$parquetPath/$ticket")
    val dfsignal: DataFrame = Signal.getSignalDF(parquet, signals: _*)
    finance[T](ticket, s"$dataPath/$ticket", dfsignal)
  }

  final def getFinancialSignal[T <: AnalyzedFinanceObject](ticket: String, conf: SignalConf): T = {
    val parquet: DataFrame = Read.parquet(s"$parquetPath/$ticket")
    val dfsignal: DataFrame = Signal.getSignalDF(parquet, conf.ss.map(_.signal): _*)
    finance[T](ticket, s"$dataPath/$ticket", dfsignal)
  }

  final def getFinancialSignals[T <: AnalyzedFinanceObject](signals: SignalIndicator*): List[T] = {
    val files: Array[String] = fileName(parquetPath)
    val parquets: Array[DataFrame] = files.map(file => Read.parquet(s"$parquetPath/$file"))
    val dfs: Array[DataFrame] = parquets.map(df => Signal.getSignalDF(df, signals: _*))
    files.zip(dfs).map { case (fname, df) => finance[T](fname, s"$dataPath/$fname", df) }.toList
  }

  def transformation[T <: AnalyzedFinanceObject](objs: T*): Unit =
    objs.foreach(obj => Write(obj.df.repartition(6)).parquet(obj.path))

  def getFinancialSignalDF(ticket: String): DataFrame = Read.parquet(s"$dataPath/$ticket")

}
