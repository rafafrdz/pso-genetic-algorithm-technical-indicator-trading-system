package mf.dabi.pso.techIndicatorTradingSystem.finance.indicators

import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{lit, udf}
import org.apache.spark.sql.types.DecimalType

import scala.util.{Random, Try}

trait Weight extends Serializable {
  self: SignalIndicator =>

  lazy val refWeight: String = s"${ref}_w"
  lazy val weight: Column = udfRndm(lit(1)).cast(DecimalType(20, 6)).as(refWeight)

  def rndm(): Double = {
    val rnmDouble: Double = Random.nextDouble
    if (Random.nextBoolean) rnmDouble else -rnmDouble
  }

  val lambdardnm: Int => Option[Double] = (_: Int) => Try {
    val rnmDouble: Double = Random.nextDouble
    if (Random.nextBoolean) rnmDouble else -rnmDouble
  }.toOption

  val udfRndm: UserDefinedFunction = udf(lambdardnm)
}
