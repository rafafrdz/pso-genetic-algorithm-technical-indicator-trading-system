package mf.dabi.pso.techIndicatorTradingSystem.finance.indicators

import mf.dabi.pso.techIndicatorTradingSystem.finance.indicators.sets._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame}

trait Signal {
  self: Indicator =>

  lazy val refSignal: String = s"${ref}_signal"
  val signal: Column
}

object Signal {

  val indd1: List[SignalIndicator] = List(SMA20, SMA100, SMA200, SMA500, MACD, RSI, STO, WLLR)
  val indd2: List[SignalIndicator] = List(SMA100, MACD, RSI, WLLR)
  val indd3: List[SignalIndicator] = List(SMA100, MACD, RSI,STO, WLLR)

  def getIndicators(df: DataFrame, indicators: Indicator*): DataFrame = {
    val persist: DataFrame = df.persist()
    indicators.map(ind => ind.calculate(persist)).fold(persist)(_.join(_, "id"))
  }

  def getSignalDF(df: DataFrame, signals: SignalIndicator*): DataFrame = {
    val persist: DataFrame = getIndicators(df, signals: _*)
    val cols: Array[Column] = persist.columns.map(f => persist(f))
    val signalsCols: Seq[Column] = signals.map(s => s.signal)
    val allCols: Array[Column] = cols ++ signalsCols
    persist.select(allCols: _*)
  }

  /** TODO. Fixear todo esto para que ese mejor modularizado */

  def getWeightDF(signalDF: DataFrame, conf: SignalConf): DataFrame = {
    val signalConf = conf.ss
    val signals: Seq[SignalIndicator] = signalConf.map(c => c.signal)

    val cols: Array[Column] = signalDF.columns.map(f => signalDF(f))
    val weightRawCols: Seq[Column] = signalConf.map(c => if(c.signal.refWeight.contains("amount")) c.signal.customWeight(math.abs(c.weight)) else c.signal.customWeight(c.weight))
    val sumWeight: Column = signals.map(s => col(s.refWeightRaw)).reduce(_ + _)
    val weightCols: Seq[Column] = signals.map(s => s.weight(sumWeight))

    val preCols: Array[Column] = cols ++ weightRawCols
    val allCols: Array[Column] = cols ++ weightCols
    signalDF.select(preCols: _*).select(allCols: _*)
  }

  def getSignal(df: DataFrame, signals: SignalIndicator*): DataFrame = {
    val persist: DataFrame = getIndicators(df, signals: _*)
    val cols: Array[Column] = persist.columns.map(f => persist(f))
    val signalsCols: Seq[Column] = signals.map(s => s.signal)

    val weightRawCols: Seq[Column] = signals.map(s => s.weightRaw)
    val sumWeight: Column = signals.map(s => col(s.refWeightRaw)).reduce(_ + _)
    val weightCols: Seq[Column] = signals.map(s => s.weight(sumWeight))

    val preCols: Array[Column] = cols ++ signalsCols ++ weightRawCols
    val allCols: Array[Column] = cols ++ signalsCols ++ weightCols
    persist.select(preCols: _*).select(allCols: _*)
  }

  case class SignalConf(ss: SignalSet*)

  case class SignalSet(signal: SignalIndicator, weight: Double)

  def getSignal(df: DataFrame, signalConf: SignalConf): DataFrame = getSignalConf(df, signalConf.ss: _*)

  def getSignalConf(df: DataFrame, signalConf: SignalSet*): DataFrame = {
    val signals: Seq[SignalIndicator] = signalConf.map(c => c.signal)

    val persist: DataFrame = getIndicators(df, signals: _*)
    val cols: Array[Column] = persist.columns.map(f => persist(f))
    val signalsCols: Seq[Column] = signals.map(s => s.signal)

    val weightRawCols: Seq[Column] = signalConf.map(c => c.signal.customWeight(c.weight))
    val sumWeight: Column = signals.map(s => col(s.refWeightRaw)).reduce(_ + _)
    val weightCols: Seq[Column] = signals.map(s => s.weight(sumWeight))

    val preCols: Array[Column] = cols ++ signalsCols ++ weightRawCols
    val allCols: Array[Column] = cols ++ signalsCols ++ weightCols
    persist.select(preCols: _*).select(allCols: _*)
  }

}

