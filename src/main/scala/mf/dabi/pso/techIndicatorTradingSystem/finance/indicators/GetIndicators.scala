package mf.dabi.pso.techIndicatorTradingSystem.finance.indicators

import mf.dabi.pso.techIndicatorTradingSystem.settings.Sparkable
import org.apache.spark.sql.DataFrame

object GetIndicators extends Sparkable {

  val df: DataFrame = Read.parquet("src/main/resources/historical-data/parquet/stocks/AAL")
  def main(args: Array[String]): Unit = {

    val algo = EMA12.calculate(df)
    val algo2 = EMA26.calculate(df)
    algo.show(false)
    algo2.show(false)
  }

}
