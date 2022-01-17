package mf.dabi.pso.techIndicatorTradingSystem.finance.indicators

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame}

trait Signal {
  self: Indicator =>

  lazy val refSignal: String = s"${ref}_signal"
  val signal: Column
}

object Signal {

  val indd1: List[SignalIndicator] = List(SMA20, SMA100, SMA200, SMA500, MACD, RSI, STO, WLLR)

  def getIndicators(df: DataFrame, indicators: Indicator*): DataFrame = {
    val persist: DataFrame = df.persist()
    indicators.map(ind => ind.calculate(persist)).fold(persist)(_.join(_, "id"))
  }

  def getSignal(df: DataFrame, signals: SignalIndicator*): DataFrame = {
    val persist: DataFrame = getIndicators(df, signals: _*)
    val cols: Array[Column] = persist.columns.map(f => persist(f))
    val signalsCols: Seq[Column] = signals.map(s => s.signal)

    val weightRawCols: Seq[Column] = signals.map(s => s.weightRaw)
    val sumWeight: Column = signals.map(s => col(s.refWeightRaw)).reduce(_+_)
    val weightCols: Seq[Column] = signals.map(s => s.weight(sumWeight))

    val preCols: Array[Column] = cols ++ signalsCols ++ weightRawCols
    val allCols: Array[Column] = cols ++ signalsCols ++ weightCols
    persist.select(preCols: _*).select(allCols:_*)
  }

}

