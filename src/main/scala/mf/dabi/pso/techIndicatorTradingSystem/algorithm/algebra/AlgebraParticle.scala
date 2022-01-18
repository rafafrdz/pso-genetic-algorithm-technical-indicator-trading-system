package mf.dabi.pso.techIndicatorTradingSystem.algorithm.algebra

import mf.dabi.pso.techIndicatorTradingSystem.algorithm.algebra.AlgebraParticle.{div, mult, res, sum}
import mf.dabi.pso.techIndicatorTradingSystem.algorithm.space.SearchSpace.Particle
import mf.dabi.pso.techIndicatorTradingSystem.finance.indicators.SignalIndicator
import mf.dabi.pso.techIndicatorTradingSystem.implicits._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.{Column, DataFrame}

trait AlgebraParticle[T] {
  protected val df: DataFrame

  def pure(df: DataFrame): T

  def *(p: Particle)(implicit indicator: List[SignalIndicator]): T = pure(mult(df, p))

  def +(p: Particle)(implicit indicator: List[SignalIndicator]): T = pure(sum(df, p))

  def -(p: Particle)(implicit indicator: List[SignalIndicator]): T = pure(res(df, p))

  def /(p: Particle)(implicit indicator: List[SignalIndicator]): T = pure(div(df, p))

}

object AlgebraParticle {
  def ope(df: DataFrame, p: Particle)(cOpe: (Column, Column) => Column)(implicit indicator: List[SignalIndicator]): DataFrame = {
    require(p.length == indicator.length, "Dimension de particula distinta a los indicadores")
    val colss: List[Column] = indicator.zip(p.data).map { case (i, v) => cOpe(df(i.refWeight), lit(v).cast(DecimalType(20, 6))).as(i.refWeight) }
    df.addCol(true, colss: _*)
  }

  def mult(simulation: DataFrame, p: Particle)(implicit indicator: List[SignalIndicator]): DataFrame = ope(simulation, p)((c1: Column, c2: Column) => c1 * c2)

  def sum(simulation: DataFrame, p: Particle)(implicit indicator: List[SignalIndicator]): DataFrame = ope(simulation, p)((c1: Column, c2: Column) => c1 + c2)

  def res(simulation: DataFrame, p: Particle)(implicit indicator: List[SignalIndicator]): DataFrame = ope(simulation, p)((c1: Column, c2: Column) => c1 - c2)

  def div(simulation: DataFrame, p: Particle)(implicit indicator: List[SignalIndicator]): DataFrame = ope(simulation, p)((c1: Column, c2: Column) => c1 / c2)
}

