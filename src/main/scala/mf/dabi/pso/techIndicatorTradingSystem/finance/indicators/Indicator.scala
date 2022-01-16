package mf.dabi.pso.techIndicatorTradingSystem.finance.indicators

import org.apache.spark.sql.{Column, DataFrame}

trait Indicator extends Serializable {
  val name: String
  val ref: String
  protected val id: String = "id"
  protected val close: String = "close"
  protected val idAux: String = s"${id}_aux"
  protected val closeAux: String = s"${close}_aux"

  def calculate(df: DataFrame): DataFrame

  sealed trait TimeOperation

  trait TimeRequest extends TimeOperation

  case object DelayRequest extends TimeRequest

  case object IntervalRequest extends TimeRequest

  case class Delay(dfIndicator: DataFrame, dfAux: DataFrame, delay: DataFrame) extends TimeOperation {
    def deploy(): (DataFrame, DataFrame, DataFrame) = (dfIndicator, dfAux, delay)
  }

  case class Interval(dfIndicator: DataFrame, dfAux: DataFrame, interval: DataFrame) extends TimeOperation {
    def deploy(): (DataFrame, DataFrame, DataFrame) = (dfIndicator, dfAux, interval)
  }

  protected final def timeOp(df: DataFrame, range: Int, request: TimeRequest): TimeOperation = request match {
    case DelayRequest => delay(df, range)
    case IntervalRequest => interval(df, range)
  }


  protected def delay(df: DataFrame, range: Int = 1): Delay = {
    val dfIndicator: DataFrame = df.select(id, close)
    val dfAux: DataFrame = dfIndicator.withColumn(idAux, dfIndicator(id)).withColumn(closeAux, dfIndicator(close))

    val joinExpr: Column = (dfIndicator(id) - dfAux(idAux)) === range
    val join: DataFrame = dfIndicator.join(dfAux, joinExpr).where(dfIndicator(id).geq(range))
    Delay(dfIndicator, dfAux, join)
  }

  protected def interval(df: DataFrame, range: Int): Interval = {
    val dfIndicator: DataFrame = df.select(id, close)
    val dfAux: DataFrame = dfIndicator.withColumn(idAux, dfIndicator(id)).withColumn(closeAux, dfIndicator(close))

    val joinExpr: Column = (dfIndicator(id) - dfAux(idAux)).isin((1 to range): _*)
    val join = dfIndicator.join(dfAux, joinExpr).where(dfIndicator(id).geq(range))
    Interval(dfIndicator, dfAux, join)
  }
}

trait SignalIndicator extends Indicator with Signal with Weight