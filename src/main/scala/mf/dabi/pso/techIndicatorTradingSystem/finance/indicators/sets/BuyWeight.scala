package mf.dabi.pso.techIndicatorTradingSystem.finance.indicators.sets

import mf.dabi.pso.techIndicatorTradingSystem.finance.indicators.SignalIndicator
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{Column, DataFrame}

case class BuyWeight(amount: Long) extends SignalIndicator {
  override val name: String = "Amount"
  override val ref: String = s"$name".toLowerCase

  override def calculate(df: DataFrame): DataFrame = df.select(df(id), lit(amount).cast(DoubleType).as(ref))

  override val signal: Column = lit(1).as(refSignal)
}
