package mf.dabi.pso.techIndicatorTradingSystem.finance.indicators

import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.{Column, DataFrame}

sealed case class SMA(period: Int) extends MA {
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

}

object SMA20 extends SMA(20)

object SMA100 extends SMA(100)

object SMA200 extends SMA(200)

object SMA500 extends SMA(500)
