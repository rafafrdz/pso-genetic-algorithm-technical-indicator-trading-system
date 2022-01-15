package mf.dabi.pso.techIndicatorTradingSystem.finance.indicators

import org.apache.spark.sql.functions.{last, lit, max, min}
import org.apache.spark.sql.{Column, DataFrame}

sealed case class GSTO(period: Int) extends Indicator {
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
}

object STO extends GSTO(14)

