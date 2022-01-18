package mf.dabi.pso.techIndicatorTradingSystem.finance.data

import mf.dabi.pso.techIndicatorTradingSystem.finance.data.adt.CryptoObject
import mf.dabi.pso.techIndicatorTradingSystem.finance.data.ingestion.Schema.{Decimal, Timestamp}
import mf.dabi.pso.techIndicatorTradingSystem.finance.data.ingestion.Schema
import mf.dabi.pso.techIndicatorTradingSystem.finance.data.simulation.Simulation
import mf.dabi.pso.techIndicatorTradingSystem.finance.indicators.Signal.indd1
import mf.dabi.pso.techIndicatorTradingSystem.settings.Sparkable
import org.apache.spark.sql.types.StructType

object Cryptos extends Sparkable with Simulation {

  def folder: String = "cryptos"

  val schema: StructType = Schema.getFrom[CryptoSchema]
  lazy val cryptos: List[CryptoObject] = getFinancialObject[CryptoObject]
  lazy val cryptosSignals: List[CryptoObject] = getFinancialSignals[CryptoObject](indicators: _*)

  case class CryptoSchema(unix: Long, date: Timestamp, symbol: String, open: Decimal, high: Decimal, low: Decimal, close: Decimal, volumeBTC: Decimal, volumeUSD: Decimal, tradecount: Int)

  def main(args: Array[String]): Unit = {
    ingest(cryptos: _*)
    transformation(cryptosSignals: _*)
  }
}
