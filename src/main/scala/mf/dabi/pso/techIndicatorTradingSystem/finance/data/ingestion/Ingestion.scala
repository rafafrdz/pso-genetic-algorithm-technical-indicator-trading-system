package mf.dabi.pso.techIndicatorTradingSystem.finance.data.ingestion

import mf.dabi.pso.techIndicatorTradingSystem.finance.data.adt.AnalyzedFinanceObject
import mf.dabi.pso.techIndicatorTradingSystem.finance.data.adt.FinanceObject.finance
import mf.dabi.pso.techIndicatorTradingSystem.settings.Sparkable
import mf.dabi.pso.techIndicatorTradingSystem.utils.DataFrameSuite.{adaptSchema, asignarId}
import mf.dabi.pso.techIndicatorTradingSystem.utils.FileSystemSuite.fileName
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

trait Ingestion {
  self: Sparkable =>

  val csvPath: String = fsPath("csv")
  val parquetPath: String = fsPath("parquet")
  val schema: StructType

  def folder: String

  protected def fsPath(fileType: String): String = s"src/main/resources/historical-data/$fileType/$folder"

  final def getFinancialObject[T <: AnalyzedFinanceObject]: List[T] = {
    val files: Array[String] = fileName(csvPath)
    val raw: Array[DataFrame] = files.map(file => Read.csv(s"$csvPath/$file"))
    val dfs: Array[DataFrame] = raw.map(df => asignarId(adaptSchema(df, schema), "date"))
    files.zip(dfs).map { case (fname, df) => finance[T](fname, "", df) }.toList
  }

  def ingest[T <: AnalyzedFinanceObject](objs: T*): Unit = objs
    .map(obj => finance[T](obj.fileName, parquetPath, obj.df))
    .foreach(obj => Write(obj.df.repartition(6)).parquet(obj.output))
}
