package mf.dabi.pso.techIndicatorTradingSystem.finance.indicators

import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.types.DecimalType

import scala.util.{Random, Try}

trait Weight extends Serializable {
  self: SignalIndicator =>

  lazy val refWeight: String = s"${ref}_weight"
  lazy val refWeightRaw: String = s"${refWeight}_raw"

  /** Version Random a nivel de fila (para cada dia) */
  //  lazy val weightRaw: Column = udfRndm(lit(1)).cast(DecimalType(20, 6)).as(refWeightRaw)

  /** Version Random a nivel de columna */
  lazy val weightRaw: Column = lit(Random.nextInt(10).asInstanceOf[Double]).cast(DecimalType(20, 6)).as(refWeightRaw)
  lazy val weight: Column => Column = (sumWeight: Column) => (col(refWeightRaw) / sumWeight).cast(DecimalType(20, 6)).as(refWeight)

  def customWeight(w: Double): Column = lit(w).cast(DecimalType(20, 6)).as(refWeightRaw)

  val lambdardnm: Int => Option[Double] = (_: Int) => Try(Random.nextInt(10).asInstanceOf[Double]).toOption

  val udfRndm: UserDefinedFunction = udf(lambdardnm)
}
