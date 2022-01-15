package mf.dabi.pso.techIndicatorTradingSystem.finance.data.ingestion

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.{DecimalType, StructField, StructType}

import scala.reflect.runtime.universe.TypeTag


object Schema {


  type Date = java.sql.Date
  type Decimal = java.math.BigDecimal
  type Timestamp = java.sql.Timestamp

  def getFrom[T <: Product : TypeTag]: StructType = adjust(Encoders.product[T].schema)

  private def adjust(schema: StructType): StructType = {
    val schAdj: Seq[StructField] = schema.map {
      case StructField(name, _: DecimalType, nullable, metadata) => StructField(name, DecimalType(20, 6), nullable, metadata)
      case field => field
    }
    StructType(schAdj)
  }


}