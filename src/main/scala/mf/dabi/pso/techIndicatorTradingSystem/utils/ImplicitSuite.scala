package mf.dabi.pso.techIndicatorTradingSystem.utils

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GetArrayStructFields, GetStructField, Literal, PrettyAttribute}
import org.apache.spark.sql.catalyst.util.{toPrettySQL, usePrettyExpression}
import org.apache.spark.sql.types.{NumericType, StringType}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.unsafe.types.UTF8String

import scala.util.Try

object ImplicitSuite {
  implicit class DataFrameImplicit(df: DataFrame) {
    def addCol(col: Column*): DataFrame = DataFrameSuite.addCol(df, col: _*)

    def addCol(remplace: Boolean, col: Column*): DataFrame = DataFrameSuite.addCol(df, remplace, col: _*)
  }


  /**
   * Implicit class to add custom column methods into Spark Sql API
   *
   * @param col column
   */

  implicit class ImplicitColumn(col: Column) {
    private val AS1: String = " AS "
    private val AS2: String = " AS `"

    private def prettyName(name: String): String = Try(prettyName1(name)).getOrElse(prettyName2(name))
    private def prettyName1(name: String): String = {
      name.split(AS1) match {
        case Array(_, alias) => alias.replace("`", "")
        case Array(name) => name
      }
    }
    private def prettyName2(name: String): String = name.split(AS2).last.replace("`", "")

    def getName: String = {
      val expr = col.expr
      val name: String = toPrettySQL(expr)
      prettyName(name)
    }

    def withEndfix(endfix: String): Column = col.alias(getName + endfix)

    def hasAny(value: Any*): Column = {
      if (value.size == 1) col.contains(value.head)
      colContains(col, value)
    }

    def colContains(values: Any*): Column =
      values.map(value => col.contains(value)).reduce(_ || _)

  }
}
