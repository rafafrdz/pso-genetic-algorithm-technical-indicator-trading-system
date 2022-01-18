package mf.dabi.pso.techIndicatorTradingSystem.utils

import mf.dabi.pso.techIndicatorTradingSystem.utils.ImplicitSuite._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame}

object DataFrameSuite {

  def adaptSchema(df: DataFrame, schema: StructType): DataFrame = {
    val schemaraw: StructType = df.schema
    val cols = schemaraw.zip(schema).map { case (oldf, newf) => df(oldf.name).cast(newf.dataType).as(newf.name) }
    df.select(cols: _*)
  }

  def asignarId(df: DataFrame, orderBy: String*): DataFrame = {
    val cols: Seq[Column] = orderBy.map(f => df(f))
    val window = Window.orderBy(cols: _*)
    df.withColumn("id", row_number().over(window))
  }

  def addCol(df: DataFrame, remplace: Boolean, col: Column*): DataFrame = {
    lazy val fieldName = col.map(c => c.getName)
    lazy val columns = df.columns.filterNot(c => fieldName.contains(c)).map(c => df(c))
    lazy val dfNoCol = df.select(columns: _*)

    if (remplace) addCol(dfNoCol, col: _*) else addCol(df, col: _*)
  }

  def addCol(df: DataFrame, col: Column*): DataFrame = {
    val origincols: Array[Column] = df.columns.map(df(_))
    val allCols: Array[Column] = origincols ++ col
    df.select(allCols: _*)
  }
}
