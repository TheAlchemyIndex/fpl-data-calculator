package util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object DataFrameHelper {

  def fromSnakeCaseToCamelCase(cols: Array[String]): Array[String] = {
    val camelCols = cols.map { colName =>
      val splitColName = colName.split("_")
      val first = splitColName.head
      val rest = splitColName.tail.map(_.capitalize)
      (first +: rest).mkString("")
    }
    camelCols
  }

  def renameColumns(df: DataFrame): DataFrame = {
    val camelCols = fromSnakeCaseToCamelCase(df.columns)

    val renamedDf = df.select(df.columns.zip(camelCols).map {
      case (a, b) => col(a).as(b)
    }: _*)

    renamedDf
  }
}
