package providers.fixtures

import constants.{CommonColumns, FixturesColumns, GameweekColumns}
import org.apache.spark.sql.DataFrame
import providers.Provider
import util.DataFrameHelper.renameColumnsToCamelCase
import util.DateFormatter.timestampToDate

class FixturesProvider(fixturesDf: DataFrame) extends Provider {

  def getData: DataFrame = {
    val camelCaseDf: DataFrame = renameColumnsToCamelCase(fixturesDf)
    val timestampToDateDf: DataFrame = timestampToDate(camelCaseDf, CommonColumns.DATE, GameweekColumns.KICKOFF_TIME)
    dropColumns(timestampToDateDf)
  }

  private def dropColumns(df: DataFrame): DataFrame = {
    df.drop(FixturesColumns.CODE,
      FixturesColumns.PROVISIONAL_START_TIME,
      FixturesColumns.KICKOFF_TIME,
      FixturesColumns.MINUTES,
      FixturesColumns.KICKOFF_TIME,
      FixturesColumns.FINISHED,
      FixturesColumns.STARTED,
      FixturesColumns.FINISHED_PROVISIONAL,
      FixturesColumns.STATS,
      FixturesColumns.ID,
      FixturesColumns.EVENT)
  }
}
