package mf.dabi.pso.techIndicatorTradingSystem.algorithm

import mf.dabi.pso.techIndicatorTradingSystem.algorithm.particle.{ParticleSolution, ParticleTraveller}
import mf.dabi.pso.techIndicatorTradingSystem.algorithm.space.SearchSpace
import mf.dabi.pso.techIndicatorTradingSystem.algorithm.space.SearchSpace.{Particle, Swarm, SwarmSolution, SwarmTraveller}
import mf.dabi.pso.techIndicatorTradingSystem.finance.data.ingestion.Simulation
import mf.dabi.pso.techIndicatorTradingSystem.finance.indicators.SignalIndicator
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.{Column, DataFrame}

import scala.annotation.tailrec

object PSOAlgorithm {

  import dfimplicit._

  /**
   * Particle Swarm Optimization (PSO) algorithm apply to financial product from market stock
   *
   * @param eval       num. of evaluations
   * @param swarmSize  size of particle swarm
   * @param space      SearchSpace where particles live in
   * @param omega      momentum factor
   * @param phip       strenght of attraction factor to best particle in the one particle's history
   * @param phig       strenght of attraction factor respect all best particle in the swarm
   * @param ticker     Ticker of financial product
   * @param indicators Indicatos to analyze
   * @param f          fitness function
   * @return
   */
  def algorithm(eval: Int, swarmSize: Int, space: SearchSpace)
               (omega: Double, phip: Double, phig: Double)
               (ticker: String, indicators: List[SignalIndicator], f: DataFrame => Double): ParticleSolution = {

    implicit val ind: List[SignalIndicator] = indicators
    val swarm0: SwarmTraveller = swarmT(swarmSize, space)(ticker, indicators)

    def stepParticle(psi: ParticleSolution, bstp: ParticleSolution): ParticleTraveller = {
      val (rp, rg, w): (Double, Double, Particle) = (SearchSpace.rndm01, SearchSpace.rndm01, space.rndmWeight)
      val vk: Particle = (omega * psi.velocity) + (phip * rp * (psi.weight - w)) + (phig * rg * (bstp.weight - w))
      val vkBounded: Particle = SearchSpace.boundParticle(vk, space.bound11)

      val wk: Particle = w + vkBounded
      psi.setWeight(wk).setVelocity(vkBounded).traveller()
    }

    @tailrec
    def stepSwarm(swarm: SwarmSolution, bstp: ParticleSolution, acc: SwarmTraveller): SwarmTraveller = {
      swarm match {
        case ps if ps.isEmpty => acc
        case p +: ps =>
          val newAcc = stepParticle(p, bstp) +: acc
          stepSwarm(ps, bstp, newAcc)
      }
    }

    @tailrec
    def aux(acc: Int)(swarm: SwarmTraveller): ParticleSolution = {
      val tradings: SwarmSolution = doTrading(swarm, f, indicators)
      val bestParticle: ParticleSolution = tradings.reduceLeft((acc, p) => optimalPS(acc, p))

      lazy val swarmSk: SwarmTraveller = stepSwarm(tradings, bestParticle, Vector.empty[ParticleTraveller])
      if (acc == 0) bestParticle else aux(acc - 1)(swarmSk)
    }

    aux(eval)(swarm0)
  }



  // //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // //////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def swarmT(swarmSize: Int, space: SearchSpace)(ticker: String, indicators: List[SignalIndicator]): SwarmTraveller =
    space.swarm(swarmSize)
      .zip(space.swarmV(swarmSize))
      .map { case (w, vel) =>
        ParticleTraveller(w, vel, Simulation.stock(ticker, w, indicators: _*))
      }
      .map(pt =>
        ParticleTraveller(pt.weight, pt.velocity, addVelCol(pt.simulation, pt.velocity, indicators)))


  def doTrading(initials: SwarmTraveller, f: DataFrame => Double, indicators: List[SignalIndicator]): SwarmSolution = {
    initials.map(ps =>
      ParticleTraveller(ps.weight, ps.velocity, TradingFunction.tradingFunction(ps.simulation, indicators: _*)))
      .map(pt =>
        ParticleSolution(pt.weight, pt.velocity, f(pt.simulation), pt.simulation))

  }

  def optimal(p1: Particle, p2: Particle, f: Particle => Double): Particle =
    if (f(p1) < f(p2)) p1 else p2

  def optimalPS(p1DF: ParticleSolution, p2DF: ParticleSolution): ParticleSolution =
    if (p1DF.result < p2DF.result) p1DF else p2DF

  def optimalPT(p1DF: ParticleTraveller, p2DF: ParticleTraveller, f: DataFrame => Double): ParticleTraveller =
    if (f(p1DF.simulation) < f(p2DF.simulation)) p1DF else p2DF

  def optimalSimulation(p1DF: DataFrame, p2DF: DataFrame, f: DataFrame => Double): DataFrame =
    if (f(p1DF) < f(p2DF)) p1DF else p2DF

  def addVelCols(initials: SwarmTraveller, velSwarm: Swarm, indicators: List[SignalIndicator]): SwarmTraveller =
    initials.zip(velSwarm)
      .map { case (traveller, velocity) =>
        ParticleTraveller(traveller.weight, velocity, addVelCol(traveller.simulation, velocity, indicators))
      }

  def addVelCol(df: DataFrame, velocity: Particle, indicators: List[SignalIndicator]): DataFrame = {
    val velocitycolumn: List[Column] = indicators.zip(velocity.data)
      .map { case (indicator, w) =>
        lit(w).cast(DecimalType(20, 6)).as(s"${indicator.refWeight}_vel")
      }

    df.addCol(true, velocitycolumn: _*)
  }

}
