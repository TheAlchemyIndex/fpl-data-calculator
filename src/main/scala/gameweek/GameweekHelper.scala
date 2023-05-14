package gameweek

import constants.GameweekColumns
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, when}

object GameweekHelper {

  def booleanColumnToBinary(df: DataFrame, newColumn: String, targetColumn: String): DataFrame = {
    df.withColumn(newColumn, when(col(targetColumn) === true, 1)
        when(col(targetColumn) === false, 0))
  }

  def dropColumns(df: DataFrame): DataFrame = {
    df.drop(GameweekColumns.TRANSFERS_BALANCE,
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
