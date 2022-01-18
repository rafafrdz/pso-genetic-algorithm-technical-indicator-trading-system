package mf.dabi.pso.techIndicatorTradingSystem.utils

import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.{Column, DataFrame}
import org.slf4j.Logger

import scala.util.Try
import scala.{specialized => sp}

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

  implicit class ImplicitLogger(logger: Logger) {
    private lazy val sep: String = "="

    def showThrowable(exc: => Throwable, mssgs: String*): Unit = {
      mssgs.foreach(mssg => logger.error(mssg))
      logger.error(s"Exception: ${exc.getMessage}")
      //      exc.printStackTrace()
    }

    def benchmark[@sp(Int, Char, Boolean) A](func: => A): Unit = DataFrameSuite.benchmark(func)(logger)

    def benchmarkV[@sp(Int, Char, Boolean) A](func: => A): A = DataFrameSuite.benchmarkV(logger, sep)(func)


    def volumetria(df: DataFrame): Unit = LoggerCustom.loggVolumetria(df)

    def h0(mssg: String*): Unit = LoggerCustom.loggerPrettyPrinter(logger, 0, sep, mssg: _*)

    def h1(mssg: String*): Unit = LoggerCustom.loggerPrettyPrinter(logger, 1, sep, mssg: _*)

    def h2(mssg: String*): Unit = LoggerCustom.loggerPrettyPrinter(logger, 2, sep, mssg: _*)

    def h3(mssg: String*): Unit = LoggerCustom.loggerPrettyPrinter(logger, 3, sep, mssg: _*)

    def h4(mssg: String*): Unit = LoggerCustom.loggerPrettyPrinter(logger, 4, sep, mssg: _*)

    def header(mssg: String): Unit = h2(mssg)

    def h0(mssg: String, sep: String): Unit = LoggerCustom.loggerPrettyHeader(logger, mssg, 0, sep)

    def h1(mssg: String, sep: String): Unit = LoggerCustom.loggerPrettyHeader(logger, mssg, 1, sep)

    def h2(mssg: String, sep: String): Unit = LoggerCustom.loggerPrettyHeader(logger, mssg, 2, sep)

    def h3(mssg: String, sep: String): Unit = LoggerCustom.loggerPrettyHeader(logger, mssg, 3, sep)

    def h4(mssg: String, sep: String): Unit = LoggerCustom.loggerPrettyHeader(logger, mssg, 4, sep)

    def header(mssg: String, sep: String): Unit = h2(mssg, sep)
  }
}
