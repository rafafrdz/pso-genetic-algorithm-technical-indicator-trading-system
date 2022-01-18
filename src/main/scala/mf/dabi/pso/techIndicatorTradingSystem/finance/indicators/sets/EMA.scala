package mf.dabi.pso.techIndicatorTradingSystem.finance.indicators.sets

import mf.dabi.pso.techIndicatorTradingSystem.finance.indicators.Indicator
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{lag, lit}
import org.apache.spark.sql.{Column, DataFrame}

sealed case class EMA(period: Int) extends Indicator {
  val name: String = "EMA"
  val ref: String = s"$name$period".toLowerCase

  private final val factor: Double = 2.0 / (period + 1.0)

  def calculate(df: DataFrame): DataFrame = {
    val dfIndicator: DataFrame = df.select(id, close)
    val emaCol: Column = ema(dfIndicator)
    dfIndicator.select(dfIndicator(id), emaCol).where(df(id).gt(period))
  }

  def ema(df: DataFrame, field: String = close): Column = emaFunc(df(field), factor, period).as(ref)


  private def emaFunc(close: Column, factor: Double, period: Int): Column = {
    val window: WindowSpec = Window.partitionBy(lit(1)).orderBy(lit(1))

    def aux(m: Int): Column = if (m == 0) close else aux(m - 1) + lit(Math.pow(1 - factor, m)) * lag(close, m).over(window)

    lit(factor) * aux(period)
  }

  def signal(df: DataFrame): DataFrame = df // todo. find signal
}

object EMA12 extends EMA(12)

object EMA26 extends EMA(26)

