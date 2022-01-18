package mf.dabi.pso.techIndicatorTradingSystem.algorithm.particle

import mf.dabi.pso.techIndicatorTradingSystem.algorithm.TradingFunction
import mf.dabi.pso.techIndicatorTradingSystem.algorithm.algebra.Algebra
import mf.dabi.pso.techIndicatorTradingSystem.algorithm.space.SearchSpace.Particle
import mf.dabi.pso.techIndicatorTradingSystem.finance.indicators.SignalIndicator
import mf.dabi.pso.techIndicatorTradingSystem.implicits._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, DataFrame}

sealed trait ParticleNaughty

case class ParticleTraveller(weight: Particle, velocity: Particle, simulation: DataFrame) extends ParticleNaughty with Algebra[ParticleTraveller] {
  protected val df: DataFrame = simulation

  override def pure(df: DataFrame): ParticleTraveller = ParticleTraveller(weight, velocity, df)

  def setWeight(w: Particle)(implicit indicator: List[SignalIndicator]): ParticleTraveller = pure(ParticleNaughty.setWeight(simulation, w))

  def setVelocity(v: Particle)(implicit indicator: List[SignalIndicator]): ParticleTraveller = pure(ParticleNaughty.setVelocity(simulation, v))
}

case class ParticleSolution(weight: Particle, velocity: Particle, result: Double, trading: DataFrame) extends ParticleNaughty with Algebra[ParticleSolution] {
  def traveller(): ParticleTraveller = ParticleTraveller(weight, velocity, trading)

  protected val df: DataFrame = trading

  override def pure(df: DataFrame): ParticleSolution = ParticleSolution(weight, velocity, result, df)
  def dropDecision(): ParticleSolution = ParticleSolution(weight, velocity, result, trading.drop(TradingFunction.decisionField))
  def reset(): ParticleSolution = ParticleSolution(weight, velocity, result, trading.drop(TradingFunction.decisionField, TradingFunction.capital, TradingFunction.acciones))

  def setWeight(w: Particle)(implicit indicator: List[SignalIndicator]): ParticleSolution = pure(ParticleNaughty.setWeight(trading, w))

  def setVelocity(v: Particle)(implicit indicator: List[SignalIndicator]): ParticleSolution = pure(ParticleNaughty.setVelocity(trading, v))

}

object ParticleNaughty {

  def setWeight(df: DataFrame, w: Particle)(implicit indicator: List[SignalIndicator]): DataFrame = {
    val weightCol: List[Column] = indicator.zip(w.data).map{case (i, value) => lit(value).cast(df.schema(i.refWeight).dataType).as(i.refWeight)}
    df.addCol(true, weightCol: _*)
  }

  def setVelocity(df: DataFrame, velocity: Particle)(implicit indicator: List[SignalIndicator]): DataFrame = {
    val weightCol: List[Column] = indicator.zip(velocity.data).map{case (i, value) => lit(value).cast(df.schema(i.refWeight).dataType).as(i.refWeight)}
    df.addCol(true, weightCol: _*)
  }

}
