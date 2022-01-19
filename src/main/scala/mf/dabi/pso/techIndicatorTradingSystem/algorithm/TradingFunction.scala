package mf.dabi.pso.techIndicatorTradingSystem.algorithm

import mf.dabi.pso.techIndicatorTradingSystem.experiment.Experiment
import mf.dabi.pso.techIndicatorTradingSystem.finance.indicators.Signal.indd1
import mf.dabi.pso.techIndicatorTradingSystem.finance.indicators.SignalIndicator
import mf.dabi.pso.techIndicatorTradingSystem.settings.Sparkable
import mf.dabi.pso.techIndicatorTradingSystem.utils.DataFrameSuite.addCol
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{Column, DataFrame, Row}

import scala.util.Try

object TradingFunction extends Sparkable {

  /** Importante */
  private val INITIAL_CAPITAL: Double = Experiment.INITIAL_CAPITAL
  private val thresHoldV: Double = Experiment.THRES_HOLD

  val capital: String = "capital"
  val initialCapitalField: String = "initialCapital"

  val close: String = "close"
  val open: String = "open"
  val acciones: String = "acciones"
  val decisionField: String = "decision"
  val tradingField: String = "trading"
  val threasholdField: String = "threashold"
  val amountField: String = "amount"
  val amountWeightField: String = s"${amountField}_weight"
  val id: String = "id"
  val trAux: String = "tr"

  private def initialCapitalCol(minId: Int): Column = when(col(id) === minId, col(amountField))

  def getMinId(df: DataFrame): Int = df.select(min(id).as("min")).collect().apply(0).getAs[Int]("min")

  def getMaxId(df: DataFrame): Int = df.select(max(id).as("max")).collect().apply(0).getAs[Int]("max")

  def finalPortfolio(df: DataFrame): Double = {
    val maxId: Int = getMaxId(df)
    df.where(col(id) === maxId).select(capital).collect().apply(0).getAs[Double](capital)
  }

  def decision(df: DataFrame, signals: SignalIndicator*): DataFrame = addCol(df, decision(signals: _*))

  def decision(signals: SignalIndicator*): Column = {
    val num: Column = signals.map(s => col(s.refSignal) * col(s.refWeight)).reduce(_ + _)
    val den: Column = signals.map(s => col(s.refWeight)).reduce(_ + _)
    (num / (den * den)).cast(DoubleType).as(decisionField)
  }


  /** Trading Function */
  private val tradingFunc: (Double, Seq[Row]) => Option[Seq[(Int, Double, Int)]] =
    (initialCapital: Double, trading: Seq[Row]) =>  Try {

      /** todo. add criterio de compra (peso) para que no sea comprar con todo el capital o vender todas las acciones */
      def buysell(decision: Double, close: Double, capitalNow: Double, amounWeight: Double, accionesNow: Int): (Double, Int) = {
        lazy val accionesCompra: Int = math.abs((((capitalNow / 4) / close) * amounWeight).toInt)
        lazy val capitalVende: Double = capitalNow + (math.abs(amounWeight * accionesNow/4) * close)
        lazy val capitalCompra: Double = capitalNow - (accionesCompra * close)

        val acciones: Int = if (decision > thresHoldV) accionesCompra else if (decision < -thresHoldV) 0 else accionesNow
        val capital: Double = if (decision > thresHoldV) capitalCompra else if (decision < -thresHoldV) capitalVende else capitalNow

        (capital, acciones)
      }

      /** Se asume que empiezas con 0 acciones */
      val initalValue: (Int, Double, Int) = (trading.head.getAs[Int](id), initialCapital, 0)
      trading.tail
        .scanLeft(initalValue)((acc, curr) => {
          val decisionP = curr.getAs[Double](curr.fieldIndex(decisionField))
          val amounW = curr.getAs[Double](curr.fieldIndex(amountWeightField))
          val closeP = curr.getAs[Double](close)
          val (capitalNow, accionesNow) = (acc._2, acc._3)
          val (capital, acciones) = buysell(decisionP, closeP, capitalNow, amounW,  accionesNow)
          (curr.getAs[Int]("id"), capital, acciones)
        })
    }.toOption
  val tradingUDF: UserDefinedFunction = udf(tradingFunc)


  /** https://stackoverflow.com/questions/58959703/calculate-value-based-on-value-from-same-column-of-the-previous-row-in-spark */
  def trading(df: DataFrame): DataFrame = {
    val minId: Int = getMinId(df)
    val dfAux = df.withColumn(capital, initialCapitalCol(minId))
      .withColumn(close, col(close).cast(DoubleType))
      .withColumn(amountWeightField, col(amountWeightField).cast(DoubleType))

    val initialCapital: Column = first(col(capital), ignoreNulls = true).cast(DoubleType).as(initialCapitalField)
    val setData: Column = collect_list(struct(id, open, close, decisionField, amountWeightField)).as(tradingField)
    val tradingColumn: Column = tradingUDF(col(initialCapitalField), col(tradingField))
    val (idCol, capitalCol, accionesCol): (Column, Column, Column) =
      (col(s"${trAux}._1").as(id), col(s"${trAux}._2").as(capital), col(s"${trAux}._3").as(acciones))

    dfAux.orderBy(col(id).asc).groupBy(lit(1)).agg(initialCapital, setData).select(explode(tradingColumn).as(trAux)).select(idCol, capitalCol, accionesCol)
  }

  def tradingFunc(df: DataFrame, indicators: SignalIndicator*): DataFrame = {
    val decisionDF: DataFrame = decision(df, indicators: _*)
    val tradingDF: DataFrame = trading(decisionDF)
    decisionDF.join(tradingDF, id)
  }

  val profit: DataFrame => Double = df => profitFunc(df)

  def profitFunc(tradingDF: DataFrame): Double = {
    val fport: Double = finalPortfolio(tradingDF)
    (fport - INITIAL_CAPITAL) / INITIAL_CAPITAL
  }

}
