package mf.dabi.pso.techIndicatorTradingSystem.utils

import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.{Column, DataFrame}

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
    private val AS: String = " AS "

    private def prettyName(name: String): String = {
      name.split(AS) match {
        case Array(_, alias) => alias.replace("`", "")
        case Array(name) => name
      }
    }

    def getName: String = {
      val expr = col.expr
      val name: String = toPrettySQL(expr)
      prettyName(name)
    }

    def withEndfix(endfix: String): Column = col.alias(getName + endfix)

    def hasAny(value: Any*): Column = {
      if (value.size == 1) col.contains(value.head)
      colContains(col, value: _*)
    }

    def colContains(values: Any*): Column =
      values.map(value => col.contains(value)).reduce(_ || _)

  }
}
