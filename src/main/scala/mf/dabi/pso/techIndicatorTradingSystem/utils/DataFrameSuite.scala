package mf.dabi.pso.techIndicatorTradingSystem.utils

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

}
