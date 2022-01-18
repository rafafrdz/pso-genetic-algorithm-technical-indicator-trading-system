package mf.dabi.pso.techIndicatorTradingSystem.finance.indicators.sets

import mf.dabi.pso.techIndicatorTradingSystem.finance.indicators.{Buy, Hold, Sell, SignalIndicator}
import org.apache.spark.sql.functions.{col, lit, negate, when}
import org.apache.spark.sql.{Column, DataFrame}


sealed case class GRSI(period: Int) extends SignalIndicator {
  final val name: String = "RSI"
  final val ref: String = s"$name$period".toLowerCase

  private final val (u, d): (String, String) = ("u", "d")

  def calculate(df: DataFrame): DataFrame = {
    val delayObj: Delay = delay(df)
    val (dfIndicator, dfAux, delayDF): (DataFrame, DataFrame, DataFrame) = delayObj.deploy()

    /** Calcular RS */
    val diffClose: Column = dfIndicator(close) - dfAux(closeAux)
    val cond: Column = diffClose.gt(0)
    val colU: Column = when(cond, diffClose).otherwise(lit(0)).as(u)
    val colD: Column = when(cond, lit(0)).otherwise(negate(diffClose)).as(d)

    val join: DataFrame = delayDF.select(dfIndicator(id), colU, colD)
    val emaObj: EMA = EMA(period)
    val rsiCol: Column = rsi(rs(join, emaObj))
    join.select(dfIndicator(id), rsiCol)
  }

  def rs(df: DataFrame, emaObj: EMA): Column = emaObj.ema(df, u) / emaObj.ema(df, d)

  def rsi(rs: Column): Column = (lit(100) - (lit(100) / (lit(1) + rs))).as(ref)

  /** Signal.
   * paper */
  val signal: Column =
    when(col(ref).geq(70), Sell.value)
      .when(col(ref).leq(30), Buy.value)
      .otherwise(Hold.value).as(refSignal)
}

object RSI extends GRSI(14)


