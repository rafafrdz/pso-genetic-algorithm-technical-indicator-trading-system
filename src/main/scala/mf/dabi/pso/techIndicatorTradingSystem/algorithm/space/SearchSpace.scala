package mf.dabi.pso.techIndicatorTradingSystem.algorithm.space

import breeze.linalg.DenseVector
import mf.dabi.pso.techIndicatorTradingSystem.algorithm.particle.{ParticleSolution, ParticleTraveller}
import mf.dabi.pso.techIndicatorTradingSystem.algorithm.space.SearchSpace._
import mf.dabi.pso.techIndicatorTradingSystem.finance.indicators.SignalIndicator

import java.util.concurrent.ThreadLocalRandom
import scala.util.Random

trait SearchSpace {
  val dim: Int
  lazy val bound: Bound[Double] = List.fill(dim)((0, 0.9))
  lazy val vBound: Bound[Double] = bound.map { case (l0, lf) => math.abs(l0 - lf) }.map(d => (-d, d))
  lazy val indicators: List[SignalIndicator] = Nil

  /** Get a random Particle bounded by space's bound */
  def rndmX: Particle = rndmP(bound: _*)

  /** Get a random Particle bounded by space's bound */
  def rndmWeight: Particle = rndmW(dim)

  /** Get a random velocity vector bounded by `bound` */
  def rndmVel: Particle = rndmV(bound: _*)

  /** Get a random swarm of size `size` and (optional) swarm bound to inicialized */
  def swarm(size: Int): Swarm = Vector.fill(size)(rndmW(dim))


  /** Get a random "swarm" of velocity vectors */
  def swarmV(size: Int): Swarm = Vector.fill(size)(rndmVel)

  lazy val bound11: Bound[Double] = List.fill(dim)((-1.0, 1.0))
}

object SearchSpace {
  type Particle = DenseVector[Double]
  type Swarm = Vector[Particle]
  type SwarmTraveller = Vector[ParticleTraveller]
  type SwarmSolution = Vector[ParticleSolution]

  type Bound[@specialized(Int, Boolean) T] = List[Limit[T]]
  type Limit[@specialized(Int, Boolean) T] = Tuple2[T, T]


  def apply(n: Int, limit: Bound[Double]): SearchSpace = new SearchSpace {
    override val dim: Int = n
    override lazy val bound: Bound[Double] = limit
  }

  def build(indicator: SignalIndicator*): SearchSpace = apply(indicator:_*)
  private def apply(indicator: SignalIndicator*): SearchSpace = new SearchSpace {
    override val dim: Int = indicator.length
    override lazy val indicators: List[SignalIndicator] = indicator.toList
  }

  def apply(n: Int): SearchSpace = new SearchSpace {
    override val dim: Int = n
  }

  def rndmP(bound: Limit[Double]*): Particle =
    DenseVector(bound.map { case (l0, lf) => rndm(l0, lf) }: _*)

  def rndmW(dim: Int): Particle = {
    val values = (1 to dim).map(j => Random.nextInt(10).toDouble)
    val sum = values.sum
    DenseVector(values: _*) / sum
  }

  def rndmV(bound: Limit[Double]*): DenseVector[Double] = {
    val nBound: Seq[Limit[Double]] = bound.map { case (l0, lf) => math.abs(l0 - lf) }.map(d => (-d, d))
    rndmP(nBound: _*)
  }

  private def boundValue(v: Double, limit: Limit[Double]): Double =
    if (v < limit._2) math.max(limit._1, v) else limit._2

  /** Get pi if pi is within (bouni0, boundif) else boundi0 or boundif, for all i in [0, bound lenght - 1]´ */
  def boundParticle(p: Particle, bound: Bound[Double]): Particle =
    DenseVector(p.toArray.zip(bound).map { case (pi, limit) => boundValue(pi, limit) }: _*)

  /** Get a random number between `a` and `b` */
  def rndm(a: Double, b: Double): Double = {
    val (maxv, minv): (Double, Double) = (math.max(a, b), math.min(a, b))
    ThreadLocalRandom.current().nextDouble(minv, maxv)
  }

  /** Get a random number within [0,1] */
  def rndm01: Double = Random.nextDouble()
}
