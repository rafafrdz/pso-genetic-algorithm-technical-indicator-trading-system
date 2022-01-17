package mf.dabi.pso.techIndicatorTradingSystem.finance.indicators

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

/** To do nothing */
case object Hold extends Order {
  val value: Int = 0

  override def toString: String = "HOLD"
}