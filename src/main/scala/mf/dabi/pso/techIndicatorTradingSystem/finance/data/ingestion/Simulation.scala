package mf.dabi.pso.techIndicatorTradingSystem.finance.data.ingestion

import mf.dabi.pso.techIndicatorTradingSystem.algorithm.space.SearchSpace
import mf.dabi.pso.techIndicatorTradingSystem.algorithm.space.SearchSpace.Particle
import mf.dabi.pso.techIndicatorTradingSystem.finance.data.ingestion.Simulation.signalConf
import mf.dabi.pso.techIndicatorTradingSystem.finance.data.{Cryptos, Stock}
import mf.dabi.pso.techIndicatorTradingSystem.finance.indicators.Signal.{SignalConf, SignalSet, getWeightDF}
import mf.dabi.pso.techIndicatorTradingSystem.finance.indicators.SignalIndicator
import mf.dabi.pso.techIndicatorTradingSystem.settings.Sparkable
import org.apache.spark.sql.DataFrame

trait Simulation extends Sparkable with Transformation {
  def simulation(ticket: String, indicators: SignalIndicator*): DataFrame = {
    val conf = signalConf(indicators: _*)
    val signalDF: DataFrame = getFinancialSignalDF(ticket)
    getWeightDF(signalDF, conf)
  }

  /** Este es la simulacion que interesa */
  def simulation(ticket: String, particle: Particle, indicators: SignalIndicator*): DataFrame = {
    val conf = signalConf(particle, indicators: _*)
    val signalDF: DataFrame = getFinancialSignalDF(ticket)
    getWeightDF(signalDF, conf)
  }

  def simulation(ticket: String, particle: Particle, conf: SignalConf): DataFrame = {
    val signalDF: DataFrame = getFinancialSignalDF(ticket)
    val indicators = conf.ss.map(s => s.signal)
    val newConf = signalConf(particle, indicators: _*)
    getWeightDF(signalDF, newConf)
  }

  def simulation(ticket: String, conf: SignalConf): DataFrame = {
    val signalDF: DataFrame = getFinancialSignalDF(ticket)
    getWeightDF(signalDF, conf)
  }
}

object Simulation {
  def stock(ticket: String, particle: Particle, conf: SignalConf): DataFrame = Stock.simulation(ticket, particle, conf)

  def stock(ticket: String, conf: SignalConf): DataFrame = Stock.simulation(ticket, conf)

  def stock(ticket: String, indicators: SignalIndicator*): DataFrame = Stock.simulation(ticket, indicators: _*)

  def stock(ticket: String, particle: Particle, indicators: SignalIndicator*): DataFrame = Stock.simulation(ticket, particle, indicators: _*)

  def crypto(ticket: String, particle: Particle, conf: SignalConf): DataFrame = Cryptos.simulation(ticket, particle, conf)

  def crypto(ticket: String, conf: SignalConf): DataFrame = Cryptos.simulation(ticket, conf)

  def crypto(ticket: String, indicators: SignalIndicator*): DataFrame = Cryptos.simulation(ticket, indicators: _*)

  def crypto(ticket: String, particle: Particle, indicators: SignalIndicator*): DataFrame = Cryptos.simulation(ticket, particle, indicators: _*)

  def signalConf(r0: Double, r1: Double, indicator: SignalIndicator*): SignalConf = {
    val ss = indicator.map(i => SignalSet(i, SearchSpace.rndm(r0, r1)))
    SignalConf(ss: _*)
  }

  def signalConf(p: Particle, indicator: SignalIndicator*): SignalConf = {
    val ss = p.toArray.zip(indicator).map { case (w, ind) => SignalSet(ind, w) }
    SignalConf(ss: _*)
  }

  def signalConf(indicator: SignalIndicator*): SignalConf = signalConf(-10, 10, indicator: _*)
}
