package mf.dabi.pso.techIndicatorTradingSystem.finance.indicators.sets

import mf.dabi.pso.techIndicatorTradingSystem.finance.indicators.{Buy, Hold, Sell, SignalIndicator}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

sealed case class GSTO(period: Int) extends SignalIndicator {
  final val name: String = "STO"
  final val ref: String = s"$name$period".toLowerCase

  def calculate(df: DataFrame): DataFrame = {
    val intervalObj: Interval = interval(df, period)
    val (dfIndicator, dfAux, intervalDF): (DataFrame, DataFrame, DataFrame) = intervalObj.deploy()
    val stoCol: Column = sto
    intervalDF.select(dfIndicator(id), dfAux(closeAux)).groupBy(dfIndicator(id)).agg(stoCol)
  }

  def sto: Column = {
    val lowestPrice: Column = min(closeAux)
    val highestPrice: Column = max(closeAux)
    val lastClose: Column = last(closeAux)
    (lit(100) * ((lastClose - lowestPrice) / (highestPrice - lowestPrice))).as(ref)
  }

  /** Signal.
   * paper */
  val signal: Column =
    when(col(ref).geq(80), Sell.value)
      .when(col(ref).leq(20), Buy.value)
      .otherwise(Hold.value).as(refSignal)
}

object STO extends GSTO(14)

