package providers.impl.unifiedData.players

import helpers.TestHelper
import UnifiedPlayersDataProviderTestSchema.unifiedPlayersDataProviderTestStruct
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, to_date}
import providers.impl.gameweek.GameweekProviderTestSchema.gameweekProviderTestStruct
import providers.impl.understat.players.UnderstatPlayersProviderTestSchema.understatPlayersProviderTestStruct

class UnifiedPlayersDataProviderTest extends TestHelper {

  final val DATE_COL: String = "date"
  final val DATE_FORMAT = "dd/MM/yyyy"

  final val TEST_GAMEWEEK_PROVIDER_DF: DataFrame = SPARK.read
    .option("header", value = true)
    .schema(gameweekProviderTestStruct)
    .csv("src/test/resources/unifiedData/gameweek_provider_data.csv")
    .withColumn(DATE_COL, to_date(col(DATE_COL), DATE_FORMAT))

  val TEST_UNDERSTAT_PLAYERS_PROVIDER_DF: DataFrame = SPARK.read
    .option("header", value = true)
    .schema(understatPlayersProviderTestStruct)
    .csv("src/test/resources/unifiedData/understat_players_provider_data.csv")
    .withColumn(DATE_COL, to_date(col(DATE_COL), DATE_FORMAT))

  val EXPECTED_UNIFIED_PLAYERS_DATA_PROVIDER_DF: DataFrame = SPARK.read
    .option("header", value = true)
    .schema(unifiedPlayersDataProviderTestStruct)
    .csv("src/test/resources/unifiedData/unified_players_data_provider_data.csv")
    .withColumn(DATE_COL, to_date(col(DATE_COL), DATE_FORMAT))

  test("getData - It should take a gameweek and players understat dataframe, join them, calculate rolling " +
    "averages, drop columns and drop null rows") {
    val unifiedPlayersDataProviderDf = new UnifiedPlayersDataProvider(TEST_GAMEWEEK_PROVIDER_DF,
      TEST_UNDERSTAT_PLAYERS_PROVIDER_DF).getData

    assert(EXPECTED_UNIFIED_PLAYERS_DATA_PROVIDER_DF.schema === unifiedPlayersDataProviderDf.schema)
    assert(EXPECTED_UNIFIED_PLAYERS_DATA_PROVIDER_DF.collect().sameElements(unifiedPlayersDataProviderDf.collect()))
  }
}
