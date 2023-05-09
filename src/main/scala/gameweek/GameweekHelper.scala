package gameweek

import constants.{CommonColumns, GameweekColumns}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, element_at, split, when}

object GameweekHelper {

  def booleanColumnToBinary(df: DataFrame, newColumn: String, targetColumn: String): DataFrame = {
    df
      .withColumn(newColumn, when(col(targetColumn) === true, 1)
        .otherwise(0))
  }

  def splitSurname(df: DataFrame): DataFrame = {
    val splitName: Column = split(df(CommonColumns.NAME), " ")
    val surname = element_at(splitName, -1)
    df.withColumn(GameweekColumns.SURNAME, surname)
  }

  def dropColumns(df: DataFrame): DataFrame = {
    df
      .drop(GameweekColumns.TRANSFERS_BALANCE,
        GameweekColumns.OWN_GOALS,
        GameweekColumns.KICKOFF_TIME,
        GameweekColumns.RED_CARDS,
        GameweekColumns.ELEMENT,
        GameweekColumns.BPS,
        GameweekColumns.WAS_HOME,
        GameweekColumns.PENALTIES_MISSED,
        GameweekColumns.FIXTURE)
  }
}
