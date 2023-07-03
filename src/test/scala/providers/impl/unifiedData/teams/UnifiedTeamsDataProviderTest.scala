package providers.impl.unifiedData.teams

import helpers.TestHelper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, to_date}
import providers.impl.fixtures.FixturesProviderTestSchema.fixturesProviderTestStruct
import providers.impl.understat.teams.UnderstatTeamsProviderTestSchema.understatTeamsProviderTestStruct
import providers.impl.unifiedData.teams.UnifiedTeamsDataProviderTestSchema.unifiedTeamsDataProviderTestStruct

class UnifiedTeamsDataProviderTest extends TestHelper {

  final val DATE_COL: String = "date"
  final val DATE_FORMAT = "dd/MM/yyyy"

  final val TEST_FIXTURES_PROVIDER_DF: DataFrame = SPARK.read
    .option("header", value = true)
    .schema(fixturesProviderTestStruct)
    .csv("src/test/resources/unifiedData/fixtures_provider_data.csv")
    .withColumn(DATE_COL, to_date(col(DATE_COL), DATE_FORMAT))

  val TEST_UNDERSTAT_TEAMS_PROVIDER_DF: DataFrame = SPARK.read
    .option("header", value = true)
    .schema(understatTeamsProviderTestStruct)
    .csv("src/test/resources/unifiedData/understat_teams_provider_data.csv")
    .withColumn(DATE_COL, to_date(col(DATE_COL), DATE_FORMAT))

  val EXPECTED_UNIFIED_TEAMS_DATA_PROVIDER_DF: DataFrame = SPARK.read
    .option("header", value = true)
    .schema(unifiedTeamsDataProviderTestStruct)
    .csv("src/test/resources/unifiedData/unified_teams_data_provider_data.csv")
    .withColumn(DATE_COL, to_date(col(DATE_COL), DATE_FORMAT))

  test("getData - It should take a fixtures and teams understat dataframe, join them, add bothScored and " +
    "cleanSheet columns, calculate rolling averages, drop columns and drop null rows") {
    val unifiedTeamsDataProviderDf = new UnifiedTeamsDataProvider(TEST_FIXTURES_PROVIDER_DF,
      TEST_UNDERSTAT_TEAMS_PROVIDER_DF).getData

    assert(EXPECTED_UNIFIED_TEAMS_DATA_PROVIDER_DF.collect().sameElements(unifiedTeamsDataProviderDf.collect()))
  }
}
