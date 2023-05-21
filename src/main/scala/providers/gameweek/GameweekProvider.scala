package providers.gameweek

import util.DateFormatter.{dateToMonthYear, timestampToDate}
import constants.{CommonColumns, GameweekColumns}
import org.apache.spark.sql.DataFrame
import providers.Provider
import util.DataFrameHelper.{booleanColumnToBinary, renameColumnsToCamelCase}

class GameweekProvider(gameweekDf: DataFrame) extends Provider {

  def getData: DataFrame = {
    val camelCaseDf: DataFrame = renameColumnsToCamelCase(this.gameweekDf)
    val homeFixtureToBinaryDf: DataFrame = booleanColumnToBinary(camelCaseDf, GameweekColumns.HOME_FIXTURE, GameweekColumns.WAS_HOME)
    val timestampToDateDf: DataFrame = timestampToDate(homeFixtureToBinaryDf, CommonColumns.DATE, GameweekColumns.KICKOFF_TIME)
    val extractMonthYearDf: DataFrame = dateToMonthYear(timestampToDateDf, CommonColumns.DATE)
    dropColumns(extractMonthYearDf)
  }

  private def dropColumns(df: DataFrame): DataFrame = {
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
