package mf.dabi.pso.techIndicatorTradingSystem.finance.indicators

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
    val allCols: Array[Column] = cols ++ signalsCols
    persist.select(allCols: _*)
  }

}

sealed trait Order extends Serializable {
  val value: Int
}

case object Buy extends Order {
  val value: Int = 1

  override def toString: String = "BUY"
}

case object Sell extends Order {
  val value: Int = -1

  override def toString: String = "SELL"
}

case object Hold extends Order {
  val value: Int = 0

  override def toString: String = "HOLD"
}