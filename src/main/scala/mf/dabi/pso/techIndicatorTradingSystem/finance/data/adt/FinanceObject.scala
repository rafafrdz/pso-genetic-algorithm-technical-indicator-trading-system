package mf.dabi.pso.techIndicatorTradingSystem.finance.data.adt

import org.apache.spark.sql.DataFrame

sealed trait FinanceObject extends Serializable

object FinanceObject {
  def finance[T <: AnalyzedFinanceObject](fName: String, pth: String, dff: DataFrame): T = {
    val afo: AnalyzedFinanceObject = new AnalyzedFinanceObject {
      override val fileName: String = fName
      override val df: DataFrame = dff
      override val path: String = pth
    }

    afo.asInstanceOf[T]
  }
}

sealed trait AnalyzedFinanceObject extends FinanceObject {
  val fileName: String
  val df: DataFrame
  val path: String
  lazy val fname: String = fileName.replace(".csv", "")
  lazy val output: String = s"$path/$fname"
}

case class StockObject(fileName: String, path: String, df: DataFrame) extends AnalyzedFinanceObject

case class CryptoObject(fileName: String, path: String, df: DataFrame) extends AnalyzedFinanceObject