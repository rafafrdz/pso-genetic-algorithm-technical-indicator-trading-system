package mf.dabi.pso.techIndicatorTradingSystem.finance.indicators

import org.apache.spark.sql.Column

trait Signal {
  self: Indicator =>

  lazy val refSignal: String = s"${ref}_signal"
  val signal: Column
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