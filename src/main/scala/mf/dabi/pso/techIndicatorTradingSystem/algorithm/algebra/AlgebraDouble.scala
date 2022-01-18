package mf.dabi.pso.techIndicatorTradingSystem.algorithm.algebra

import mf.dabi.pso.techIndicatorTradingSystem.algorithm.algebra.AlgebraDouble.{div, mult, res, sum}
import mf.dabi.pso.techIndicatorTradingSystem.finance.indicators.SignalIndicator
import mf.dabi.pso.techIndicatorTradingSystem.implicits._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, DataFrame}

trait AlgebraDouble[T] {
  protected val df: DataFrame

  def pure(df: DataFrame): T

  def *(d: Double)(implicit indicator: List[SignalIndicator]): T = pure(mult(df, d))

  def +(d: Double)(implicit indicator: List[SignalIndicator]): T = pure(sum(df, d))

  def -(d: Double)(implicit indicator: List[SignalIndicator]): T = pure(res(df, d))

  def /(d: Double)(implicit indicator: List[SignalIndicator]): T = pure(div(df, d))

}

object AlgebraDouble {
  def ope(simulation: DataFrame, d: Double)(cOpe: (Column, Column) => Column)(implicit indicator: List[SignalIndicator]): DataFrame = {
    val colss = indicator.map(i => cOpe(simulation(i.refWeight), lit(d)).as(i.refWeight))
    simulation.addCol(true, colss: _*)
  }

  def mult(simulation: DataFrame, d: Double)(implicit indicator: List[SignalIndicator]): DataFrame = ope(simulation, d)((c1: Column, c2: Column) => c1 * c2)

  def sum(simulation: DataFrame, d: Double)(implicit indicator: List[SignalIndicator]): DataFrame = ope(simulation, d)((c1: Column, c2: Column) => c1 + c2)

  def res(simulation: DataFrame, d: Double)(implicit indicator: List[SignalIndicator]): DataFrame = ope(simulation, d)((c1: Column, c2: Column) => c1 - c2)

  def div(simulation: DataFrame, d: Double)(implicit indicator: List[SignalIndicator]): DataFrame = ope(simulation, d)((c1: Column, c2: Column) => c1 / c2)
}
