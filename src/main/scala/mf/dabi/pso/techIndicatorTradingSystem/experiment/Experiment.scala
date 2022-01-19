package mf.dabi.pso.techIndicatorTradingSystem.experiment

import mf.dabi.pso.techIndicatorTradingSystem.algorithm.particle.ParticleSolution
import mf.dabi.pso.techIndicatorTradingSystem.algorithm.space.SearchSpace
import mf.dabi.pso.techIndicatorTradingSystem.algorithm.{PSOAlgorithm, TradingFunction}
import mf.dabi.pso.techIndicatorTradingSystem.finance.data.simulation.Simulation.setAmount
import mf.dabi.pso.techIndicatorTradingSystem.finance.indicators.Signal.{indd1, indd2, indd3}
import mf.dabi.pso.techIndicatorTradingSystem.finance.indicators.SignalIndicator
import mf.dabi.pso.techIndicatorTradingSystem.settings.Sparkable
import mf.dabi.pso.techIndicatorTradingSystem.utils.DataFrameSuite

object Experiment extends Sparkable {


  val INITIAL_CAPITAL: Long = 1000000
  val THRES_HOLD: Double = 0.96


  /** Importante no borrar */
  val indicators: List[SignalIndicator] = setAmount(INITIAL_CAPITAL, indd3)
  val space: SearchSpace = SearchSpace.build(indicators: _*)
  val wi: (Double, Double) = (0.4, 0.9)
  val c1: (Double, Double) = (0.5, 2.5)
  val c2: (Double, Double) = (0.5, 2.5)


//  lazy val exp1: ParticleSolution = PSOAlgorithm.algorithm(2, 5, space)(wi, c1, c2)("AAPL", "2017-01-03", space.indicators, TradingFunction.profit)
  lazy val exp2: ParticleSolution = PSOAlgorithm.algorithm(10, 3, space)(wi, c1, c2)("AAPL", "2021-01-01", space.indicators, TradingFunction.profit)


  def main(args: Array[String]): Unit = {
    val data: ParticleSolution = DataFrameSuite.benchmark(exp2)
    Write(data.trading).parquet("src/main/resources/historical-data/results/AAPL/exp2")
    0
  }

}
