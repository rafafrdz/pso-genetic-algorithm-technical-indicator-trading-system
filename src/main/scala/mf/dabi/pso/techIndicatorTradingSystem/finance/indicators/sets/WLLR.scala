package mf.dabi.pso.techIndicatorTradingSystem.finance.indicators.sets

import mf.dabi.pso.techIndicatorTradingSystem.finance.indicators.{Buy, Hold, Sell, SignalIndicator}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

/** Info.
 * https://www.investopedia.com/terms/w/williamsr.asp
 * https://www.metatrader5.com/es/terminal/help/indicators/oscillators/wpr
 * */
sealed case class GWLLR(period: Int) extends SignalIndicator {
  final val name: String = "WllR"
  final val ref: String = s"$name$period".toLowerCase

  def calculate(df: DataFrame): DataFrame = {
    val intervalObj: Interval = interval(df, period)
    val (dfIndicator, dfAux, intervalDF): (DataFrame, DataFrame, DataFrame) = intervalObj.deploy()
    val wllrCol: Column = wllr
    intervalDF.select(dfIndicator(id), dfAux(closeAux)).groupBy(dfIndicator(id)).agg(wllrCol)
  }

  def wllr: Column = {
    val lowestPrice: Column = min(closeAux)
    val highestPrice: Column = max(closeAux)
    val lastClose: Column = last(closeAux)
    (lit(-100) * ((highestPrice - lastClose) / (highestPrice - lowestPrice))).as(ref)
  }

  /** Signal.
   * paper */
  val signal: Column =
    when(col(ref).leq(-80), Sell.value)
      .when(col(ref).geq(-20), Buy.value)
      .otherwise(Hold.value).as(refSignal)
}

object WLLR extends GWLLR(14)

