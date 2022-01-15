package mf.dabi.pso.techIndicatorTradingSystem.finance.indicators

import mf.dabi.pso.techIndicatorTradingSystem.settings.Sparkable
import org.apache.spark.sql.{Column, DataFrame}

object GetSignals extends Sparkable {

  val df: DataFrame = Read.parquet("src/main/resources/historical-data/parquet/stocks/AAL")

  val indd1: List[SignalIndicator] = List(SMA20, SMA100, SMA200, SMA500, MACD, RSI, STO, WLLR)

  def getIndicators(df: DataFrame, indicators: Indicator*): DataFrame = {
    val persist: DataFrame = df.persist()
    indicators.map(ind => ind.calculate(persist)).fold(persist)(_.join(_, "id"))
  }

  def getSignal(df: DataFrame, signals: SignalIndicator*): DataFrame = {
    val persist: DataFrame = getIndicators(df, signals: _*)
    val cols: Array[Column] = persist.columns.map(f => persist(f))
    val signalsCols: Seq[Column] = signals.map(s => s.signal)
    val allCols: Array[Column] = cols ++ signalsCols
    persist.select(allCols: _*)

  }

  def main(args: Array[String]): Unit = {

    val algo = getSignal(df, indd1: _*)
    algo.show(false)
    algo.printSchema()
  }

}
