package providers.impl.understat.teams

import helpers.TestHelper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, to_date}
import providers.impl.understat.teams.UnderstatTeamsProviderTestSchema.understatTeamsProviderTestStruct
import providers.impl.understat.teams.UnderstatTeamsTestSchema.understatTeamsTestStruct

class UnderstatTeamsProviderTest extends TestHelper {

  final val DATE_COL: String = "date"
  final val DATE_FORMAT = "yyyy/dd/MM"
  final val DROPPED_COLUMNS: Seq[String] = Seq("wins", "deep", "ppda_allowed", "missed", "ppda", "pts", "xpts", "result",
    "deep_allowed", "scored", "loses", "draws")

  val TEST_UNDERSTAT_TEAMS_DF: DataFrame = SPARK.read
    .option("header", value = true)
    .schema(understatTeamsTestStruct)
    .csv("src/test/resources/understat/understat_teams_data.csv")
    .withColumn(DATE_COL, to_date(col(DATE_COL), DATE_FORMAT))

  val EXPECTED_UNDERSTAT_TEAMS_PROVIDER_DF: DataFrame = SPARK.read
    .option("header", value = true)
    .schema(understatTeamsProviderTestStruct)
    .csv("src/test/resources/understat/understat_teams_provider_data.csv")
    .withColumn(DATE_COL, to_date(col(DATE_COL), DATE_FORMAT))

  test("getData - It should return a teams understat DataFrame with a formatted date column, team type columns " +
    "and exclude columns that were to be dropped") {
    val understatTeamsProviderDf: DataFrame = new UnderstatTeamsProvider(TEST_UNDERSTAT_TEAMS_DF).getData
    val remainingColumns: Seq[String] = understatTeamsProviderDf.columns.toSeq

    assert(EXPECTED_UNDERSTAT_TEAMS_PROVIDER_DF.schema === understatTeamsProviderDf.schema)
    assert(EXPECTED_UNDERSTAT_TEAMS_PROVIDER_DF.collect().sameElements(understatTeamsProviderDf.collect()))
    assert(DROPPED_COLUMNS.intersect(remainingColumns).isEmpty)
  }
}
