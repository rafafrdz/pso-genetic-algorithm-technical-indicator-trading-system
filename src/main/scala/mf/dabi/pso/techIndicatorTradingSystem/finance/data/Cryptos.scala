package mf.dabi.pso.techIndicatorTradingSystem.finance.data

import mf.dabi.pso.techIndicatorTradingSystem.finance.data.Schema.{Decimal, Timestamp}
import mf.dabi.pso.techIndicatorTradingSystem.finance.data.adt.CryptoObject
import mf.dabi.pso.techIndicatorTradingSystem.settings.Sparkable
import org.apache.spark.sql.types.StructType

object Cryptos extends Sparkable with Ingestion {

  def folder: String = "cryptos"

  val schema: StructType = Schema.getFrom[CryptoSchema]
  lazy val cryptos: List[CryptoObject] = getFinancialObject[CryptoObject]

  case class CryptoSchema(unix: Long, date: Timestamp, symbol: String, open: Decimal, high: Decimal, low: Decimal, close: Decimal, volumeBTC: Decimal, volumeUSD: Decimal, tradecount: Int)

  def main(args: Array[String]): Unit = {
    persist(cryptos: _*)
  }
}
