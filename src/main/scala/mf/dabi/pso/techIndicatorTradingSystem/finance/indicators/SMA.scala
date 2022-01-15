package mf.dabi.pso.techIndicatorTradingSystem.finance.indicators

import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.{Column, DataFrame}

sealed case class SMA(period: Int) extends MA {
  protected final val name: String = "SMA"
  protected final val ref: String = s"$name$period".toLowerCase
  private final val id: String = "id"
  private final val idAux: String = s"${id}_aux"
  private final val close: String = "close"
  private final val closeAux: String = "closeAux"

  def calculate(df: DataFrame): DataFrame = {
    val dfIndicator: DataFrame = df.select(id, close).where(df(id).geq(period))
    val dfAux: DataFrame = dfIndicator.withColumn(idAux, dfIndicator(id)).withColumn(closeAux, dfIndicator(close))

    val joinExpr: Column = dfIndicator(id).geq(dfAux(idAux) - period) && dfIndicator(id).lt(dfAux(idAux))
    val join = dfIndicator.join(dfAux, joinExpr).select(dfIndicator(id), dfAux(closeAux))

    val aggCol: Column = (sum(dfAux(closeAux)) / period).as(ref)
    join.groupBy(dfIndicator(id)).agg(aggCol)
  }

}

object SMA20 extends SMA(20)

object SMA100 extends SMA(100)

object SMA200 extends SMA(200)

object SMA500 extends SMA(500)
