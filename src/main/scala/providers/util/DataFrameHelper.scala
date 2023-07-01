package providers.util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, when}

object DataFrameHelper {

  def renameColumnsToCamelCase(df: DataFrame): DataFrame = {
    val camelCols: Array[String] = snakeCaseToCamelCaseColumns(df.columns)
    val renamedDf: DataFrame = df.select(df.columns.zip(camelCols).map {
      case (a, b) => col(a).as(b)
    }: _*)
    renamedDf
  }

  private def snakeCaseToCamelCaseColumns(cols: Array[String]): Array[String] = {
    val camelCols: Array[String] = cols.map { colName =>
      val splitColName: Array[String] = colName.split("_")
      val firstWord: String = splitColName.head
      val restOfWords: Array[String] = splitColName.tail.map(_.capitalize)
      (firstWord +: restOfWords).mkString("")
    }
    camelCols
  }

  def booleanColumnToBinary(df: DataFrame, newColumn: String, targetColumn: String): DataFrame = {
    df.withColumn(newColumn, when(col(targetColumn) === true, 1)
      when(col(targetColumn) === false, 0))
  }

  def dropNullRows(df: DataFrame, targetCols: Seq[String]): DataFrame = {
    df.na.drop(targetCols)
  }

  def joinDataLeftOuter(leftDf: DataFrame, rightDf: DataFrame, columns: Seq[String]): DataFrame = {
    leftDf.join(rightDf, columns, "left_outer")
      .na.fill(0)
  }
}
