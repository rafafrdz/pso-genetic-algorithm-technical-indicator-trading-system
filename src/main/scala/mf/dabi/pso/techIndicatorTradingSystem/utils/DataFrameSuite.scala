package mf.dabi.pso.techIndicatorTradingSystem.utils

import mf.dabi.pso.techIndicatorTradingSystem.utils.ImplicitSuite._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame}
import org.slf4j.Logger

import scala.{specialized => sp}
import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}

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

  def benchmark[@sp(Int, Char, Boolean) A](func: => A)(implicit logger: Logger): A = benchmarkV(logger, "#")(func)
  def benchmarkV[@sp(Int, Char, Boolean) A](logger: Logger, sep: String)(func: => A): A = {

    val t0 = Calendar.getInstance()
    val result: A = func
    val t1 = Calendar.getInstance()

    val timeZone = TimeZone.getTimeZone("GMT+2")
    val dFormat = new SimpleDateFormat("yyyy-MM-dd'  T 'HH:mm:ss.SSS' 'XXX")
    dFormat.setTimeZone(timeZone)

    // Tiempos absolutos
    val startPasoTime = t0.getTimeInMillis
    val endPasoTime = t1.getTimeInMillis

    val tiempoMiliSegundos = endPasoTime - startPasoTime
    val tiempoSegundos = tiempoMiliSegundos.toFloat / 1000

    logger.info("*************************************************************")
    logger.info("***")
    logger.info(s"***    Hora Inicio (ES)         : ${dFormat.format(t0.getTime)}")
    logger.info(s"***    Hora Fin (ES)            : ${dFormat.format(t1.getTime)}")
    logger.info(s"***    Tiempo total del proceso : ${tiempoSegundos} segundos")
    logger.info("***")
    logger.info("***")
    logger.info("*************************************************************")

    result
  }
}
