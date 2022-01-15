package mf.dabi.pso.techIndicatorTradingSystem.finance.indicators

import org.apache.spark.sql.{Column, DataFrame}

sealed case class GMACD(p1: Int, p2: Int) extends MA {
  final val name: String = "MACD"
  final val ref: String = s"$name$p1$p2".toLowerCase
  private val (pmax, pmin): (Int, Int) = (Math.max(p1, p2), Math.min(p1, p2))

  def calculate(df: DataFrame): DataFrame = {
    val emapmax: Column = EMA(pmax).ema(df)
    val emapmin: Column = EMA(pmin).ema(df)
    val macdp1p2: Column = (emapmin - emapmax).as(ref)
    df.select(df(id), macdp1p2).where(df(id).gt(pmax))
  }
}

object MACD extends GMACD(12, 26)

