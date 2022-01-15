package mf.dabi.pso.techIndicatorTradingSystem.finance.data

import mf.dabi.pso.techIndicatorTradingSystem.finance.data.Schema.{Date, Decimal}
import mf.dabi.pso.techIndicatorTradingSystem.finance.data.adt.StockObject
import mf.dabi.pso.techIndicatorTradingSystem.settings.Sparkable
import org.apache.spark.sql.types.StructType


object Stock extends Sparkable with Ingestion {

  def folder: String = "stocks"

  lazy val schema: StructType = Schema.getFrom[StockSchema]
  lazy val stocks: List[StockObject] = getFinancialObject[StockObject]

  case class StockSchema(date: Date, open: Decimal, high: Decimal, low: Decimal, close: Decimal, adj: Decimal, volume: Decimal)

  def main(args: Array[String]): Unit = {
    persist(stocks: _*)
  }
}
