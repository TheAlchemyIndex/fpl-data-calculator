package providers.impl.fixtures

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, to_date}
import providers.Provider
import util.constants.{CommonColumns, FixturesColumns}

class FixturesProvider(fixturesDf: DataFrame) extends Provider {

  def getData: DataFrame = {
    val dateFormattedDf: DataFrame = fixturesDf
      .withColumn(CommonColumns.DATE, to_date(col(FixturesColumns.KICKOFF_TIME), "yyyy-MM-dd"))

    dropColumns(dateFormattedDf)
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
