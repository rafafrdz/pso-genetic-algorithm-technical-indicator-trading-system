package mf.dabi.pso.techIndicatorTradingSystem.finance.data

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.StructType

import scala.reflect.runtime.universe.TypeTag


object Schema {


  type Date = java.sql.Date
  type Decimal = java.math.BigDecimal
  type Timestamp = java.sql.Timestamp

  def getFrom[T <: Product : TypeTag]: StructType = Encoders.product[T].schema


}