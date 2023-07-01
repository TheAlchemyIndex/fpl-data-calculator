package providers.impl.gameweek

import constants.{CommonColumns, GameweekColumns}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, month, to_date}
import providers.Provider
import providers.util.DataFrameHelper.booleanColumnToBinary

class GameweekProvider(gameweekDf: DataFrame) extends Provider {

  def getData: DataFrame = {
    val dateFormattedDf: DataFrame = booleanColumnToBinary(gameweekDf, GameweekColumns.HOME_FIXTURE, GameweekColumns.WAS_HOME)
      .withColumn(CommonColumns.DATE, to_date(col(GameweekColumns.KICKOFF_TIME), "yyyy-MM-dd"))
      .withColumn(GameweekColumns.MONTH, month(col(CommonColumns.DATE)))
      .withColumn(GameweekColumns.YEAR, month(col(CommonColumns.DATE)))

    dropColumns(dateFormattedDf)
  }

  private def dropColumns(df: DataFrame): DataFrame = {
    df.drop(GameweekColumns.TRANSFERS_BALANCE,
      GameweekColumns.OWN_GOALS,
      GameweekColumns.KICKOFF_TIME,
      GameweekColumns.RED_CARDS,
      GameweekColumns.TEAM_A_SCORE,
      GameweekColumns.ELEMENT,
      GameweekColumns.BPS,
      GameweekColumns.WAS_HOME,
      GameweekColumns.PENALTIES_MISSED,
      GameweekColumns.FIXTURE,
      GameweekColumns.TEAM_H_SCORE)
  }
}
