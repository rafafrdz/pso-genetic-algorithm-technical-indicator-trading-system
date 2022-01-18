package mf.dabi.pso.techIndicatorTradingSystem.finance.indicators.sets

import mf.dabi.pso.techIndicatorTradingSystem.finance.indicators.{Buy, Hold, Sell, SignalIndicator}
import org.apache.spark.sql.functions.{col, sum, when}
import org.apache.spark.sql.{Column, DataFrame}

/** Info.
 * https://www.investopedia.com/terms/s/sma.asp
 * */
sealed case class SMA(period: Int) extends SignalIndicator {
  final val name: String = "SMA"
  final val ref: String = s"$name$period".toLowerCase

  def calculate(df: DataFrame): DataFrame = {
    val intervalObj: Interval = interval(df, period)
    val (dfIndicator, dfAux, intervalDF): (DataFrame, DataFrame, DataFrame) = intervalObj.deploy()

    val aggCol: Column = (sum(dfAux(closeAux)) / period).as(ref)

    intervalDF
      .select(dfIndicator(id), dfAux(closeAux))
      .groupBy(dfIndicator(id)).agg(aggCol)
  }

  /** Signal.
   * paper */
  val signal: Column =
    when(col(close).gt(col(ref)), Buy.value)
      .when(col(close).lt(col(ref)), Sell.value)
      .otherwise(Hold.value).as(refSignal)
}

object SMA20 extends SMA(20)

object SMA100 extends SMA(100)

object SMA200 extends SMA(200)

object SMA500 extends SMA(500)
