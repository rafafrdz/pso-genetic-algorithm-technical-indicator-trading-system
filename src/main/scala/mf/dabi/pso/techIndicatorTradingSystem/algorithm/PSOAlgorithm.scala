package mf.dabi.pso.techIndicatorTradingSystem.algorithm

import mf.dabi.pso.techIndicatorTradingSystem.algorithm.particle.{ParticleSolution, ParticleTraveller}
import mf.dabi.pso.techIndicatorTradingSystem.algorithm.space.SearchSpace
import mf.dabi.pso.techIndicatorTradingSystem.algorithm.space.SearchSpace._
import mf.dabi.pso.techIndicatorTradingSystem.finance.data.simulation.Simulation
import mf.dabi.pso.techIndicatorTradingSystem.finance.indicators.SignalIndicator
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.{Column, DataFrame}
import org.slf4j.Logger

import scala.annotation.tailrec

object PSOAlgorithm {

  import dfimplicit._

  /**
   * Particle Swarm Optimization (PSO) algorithm apply to financial product from market stock
   *
   * @param eval       num. of evaluations
   * @param swarmSize  size of particle swarm
   * @param space      SearchSpace where particles live in
   * @param omegaG     momentum factor
   * @param phiP       strenght of attraction factor to best particle in the one particle's history
   * @param phiG       strenght of attraction factor respect all best particle in the swarm
   * @param ticker     Ticker of financial product
   * @param from       Date from which the experiment is performed. Format: yyyy-mm-dd
   * @param indicators Indicatos to analyze
   * @param f          fitness function
   * @return
   */
  def algorithm(eval: Int, swarmSize: Int, space: SearchSpace)
               (omegaG: Limit[Double], phiP: Limit[Double], phiG: Limit[Double])
               (ticker: String, from: String, indicators: List[SignalIndicator], f: DataFrame => Double)(implicit logger: Logger): ParticleSolution = {
    logger.h2("[PSO Algorithm] Initialized")

    implicit val ind: List[SignalIndicator] = indicators
    val swarm0: SwarmTraveller = swarmTFrom(swarmSize, space)(ticker, from, indicators)
    val omega: Vector[Double] = incremental(omegaG._1, omegaG._2, eval, true)
    val phip: Vector[Double] = incremental(phiP._1, phiP._2, eval)
    val phig: Vector[Double] = incremental(phiG._1, phiG._2, eval, true)


    def stepParticle(iter: Int)(psi: ParticleSolution, bstp: ParticleSolution): ParticleTraveller = {
      val (rp, rg, w): (Double, Double, Particle) = (SearchSpace.rndm01, SearchSpace.rndm01, space.rndmWeight)
      val vk: Particle = (omega(iter) * psi.velocity) + (phip(iter) * rp * (psi.weight - w)) + (phig(iter) * rg * (bstp.weight - w))
      val vkBounded: Particle = SearchSpace.boundParticle(vk, space.bound11)

      val wk: Particle = w + vkBounded
      psi.setWeight(wk).setVelocity(vkBounded).traveller()
    }

    @tailrec
    def stepSwarm(iter: Int)(swarm: SwarmSolution, bstp: ParticleSolution, acc: SwarmTraveller): SwarmTraveller = {
      swarm match {
        case ps if ps.isEmpty => acc
        case p +: ps =>
          val newAcc = stepParticle(iter)(p, bstp) +: acc
          stepSwarm(iter)(ps, bstp, newAcc)
      }
    }

    @tailrec
    def aux(acc: Int)(swarm: SwarmTraveller): ParticleSolution = {
      require(acc>0)
      val iteration: Int = eval - acc + 1
      logger.h3(s"Iteration: ${iteration}/$eval")
      val tradings: SwarmSolution = doTrading(swarm)(f, indicators)
      val bestParticle: ParticleSolution = tradings.reduceLeft((acc, p) => optimalPS(acc, p))
      lazy val tradingsW: SwarmSolution = tradings.map(p => p.reset())

      logger.h3(s"[Best Particle] Iteration: $iteration - Result: ${bestParticle.result} - Weight: ${bestParticle.weight.data.mkString(", ")}")
      lazy val swarmSk: SwarmTraveller = stepSwarm(acc - 1)(tradingsW, bestParticle, Vector.empty[ParticleTraveller])
      bestParticle.trading.show(false)
      if (acc == 1) bestParticle else aux(acc - 1)(swarmSk)
    }

    aux(eval)(swarm0)
  }



  // //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // //////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def swarmTFrom(swarmSize: Int, space: SearchSpace)(ticker: String, from: String, indicators: List[SignalIndicator]): SwarmTraveller =
    space.swarm(swarmSize)
      .zip(space.swarmV(swarmSize))
      .map { case (w, vel) =>
        ParticleTraveller(w, vel, Simulation.stock(ticker, from, indicators: _*)(w)) // todo. cambiar esto y parametrizar
      }
      .map(pt =>
        ParticleTraveller(pt.weight, pt.velocity, addVelCol(pt.simulation, pt.velocity, indicators)))

  def swarmT(swarmSize: Int, space: SearchSpace)(ticker: String, indicators: List[SignalIndicator]): SwarmTraveller =
    space.swarm(swarmSize)
      .zip(space.swarmV(swarmSize))
      .map { case (w, vel) =>
        ParticleTraveller(w, vel, Simulation.stock(ticker, w, indicators: _*)) // todo. cambiar esto y parametrizar
      }
      .map(pt =>
        ParticleTraveller(pt.weight, pt.velocity, addVelCol(pt.simulation, pt.velocity, indicators)))


  def doTrading(initials: SwarmTraveller)(f: DataFrame => Double, indicators: List[SignalIndicator]): SwarmSolution = {
    initials.map(ps =>
      ParticleTraveller(ps.weight, ps.velocity, TradingFunction.tradingFunc(ps.simulation, indicators: _*)))
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

  def incremental(a: Double, b: Double, dim: Int, reverse: Boolean = false): Vector[Double] = {
    val (maxv, minv): (Double, Double) = (math.max(a, b), math.min(a, b))
    val m: Double = (maxv - minv) / (dim - 1)
    val xs = (0 until dim).map(i => a + i * m).toVector
    if (reverse) xs.reverse else xs
  }

  def main(args: Array[String]): Unit = {
    val a = incremental(2, 5, 6)
    0
  }

}
