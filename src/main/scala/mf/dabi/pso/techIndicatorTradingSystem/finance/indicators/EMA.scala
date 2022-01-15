package mf.dabi.pso.techIndicatorTradingSystem.finance.indicators

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{lag, lit}
import org.apache.spark.sql.{Column, DataFrame}

sealed case class EMA(period: Int) extends MA {
  protected final val name: String = "EMA"
  protected final val ref: String = s"$name$period".toLowerCase
  private final val id: String = "id"
  private final val close: String = "close"

  private final val factor: Double = 2.0 / (period + 1.0)

  def calculate(df: DataFrame): DataFrame = {
    val dfIndicator: DataFrame = df.select(id, close)
    val emaCol: Column = emaFunc(dfIndicator(close), factor, period).as(ref)
    dfIndicator.select(dfIndicator(id), emaCol).where(df(id).gt(period))
  }


  private def emaFunc(close: Column, factor: Double, period: Int): Column = {
    val window: WindowSpec = Window.partitionBy(lit(1)).orderBy(lit(1))
    def aux(m: Int): Column = if (m == 0) close else aux(m - 1) + lit(Math.pow(1 - factor, m)) * lag(close, m).over(window)
    lit(factor) * aux(period)
  }

}

object EMA12 extends EMA(12)

object EMA26 extends EMA(26)
