package mf.dabi.pso.techIndicatorTradingSystem.algorithm

import mf.dabi.pso.techIndicatorTradingSystem.finance.indicators.SignalIndicator
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

object Function {


  def decision(signals: SignalIndicator*): Column = {
    val num: Column = signals.map(s => col(s.refSignal) * col(s.refWeight)).reduce(_ + _)
    val den: Column = signals.map(s => col(s.refWeight)).reduce(_ + _)
    num / den
  }



}
