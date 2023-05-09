package understat

import constants.{CommonColumns, GameweekColumns, UnderstatColumns}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{element_at, split}

object UnderstatHelper {

  def splitSurname(df: DataFrame): DataFrame = {
    val splitName: Column = split(df(CommonColumns.NAME), " ")
    val surname = element_at(splitName, -1)
    df.withColumn(GameweekColumns.SURNAME, surname)
  }

  def dropColumns(df: DataFrame): DataFrame = {
    df
      .drop(UnderstatColumns.X_G_CHAIN,
        UnderstatColumns.H_GOALS,
        UnderstatColumns.A_TEAM,
        UnderstatColumns.ROSTER_ID,
        UnderstatColumns.ASSISTS,
        UnderstatColumns.SEASON,
        UnderstatColumns.A_GOALS,
        UnderstatColumns.TIME,
        UnderstatColumns.POSITION,
        UnderstatColumns.ID,
        UnderstatColumns.H_TEAM,
        UnderstatColumns.GOALS)
  }
}
