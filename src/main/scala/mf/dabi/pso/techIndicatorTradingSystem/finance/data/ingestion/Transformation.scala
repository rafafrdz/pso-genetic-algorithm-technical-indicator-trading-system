package mf.dabi.pso.techIndicatorTradingSystem.finance.data.ingestion

import mf.dabi.pso.techIndicatorTradingSystem.finance.data.adt.AnalyzedFinanceObject
import mf.dabi.pso.techIndicatorTradingSystem.finance.data.adt.FinanceObject.finance
import mf.dabi.pso.techIndicatorTradingSystem.finance.indicators.{Signal, SignalIndicator}
import mf.dabi.pso.techIndicatorTradingSystem.settings.Sparkable
import mf.dabi.pso.techIndicatorTradingSystem.utils.FileSystemSuite.fileName
import org.apache.spark.sql.DataFrame

trait Transformation extends Ingestion {
  self: Sparkable =>

  val dataPath: String = fsPath("data")

  final def getFinancialSignals[T <: AnalyzedFinanceObject](signals: SignalIndicator*): List[T] = {
    val files: Array[String] = fileName(parquetPath)
    val parquets: Array[DataFrame] = files.map(file => Read.parquet(s"$parquetPath/$file"))
    val dfs: Array[DataFrame] = parquets.map(df => Signal.getSignal(df, signals: _*))
    files.zip(dfs).map { case (fname, df) => finance[T](fname, s"$dataPath/$fname", df) }.toList
  }

  def transformation[T <: AnalyzedFinanceObject](objs: T*): Unit =
    objs.foreach(obj => Write(obj.df.repartition(6)).parquet(obj.path))


}
