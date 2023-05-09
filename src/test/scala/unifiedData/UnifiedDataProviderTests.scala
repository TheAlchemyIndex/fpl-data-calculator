package unifiedData

import helpers.schemas.GameweekTestSchema.gameweekTestStruct
import helpers.TestHelper
import helpers.schemas.JoinedDataFilteredSchema.joinedDataFilteredStruct
import helpers.schemas.UnderstatTestSchema.understatTestStruct
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, to_date}

class UnifiedDataProviderTests extends TestHelper {

  final val UNFORMATTED_DATE_COL = "unformattedDate"
  final val DATE_FORMAT = "dd/MM/yyyy"
  final val DATE_COL = "date"

  val TEST_GAMEWEEK_DF: DataFrame = SPARK.read
    .option("header", value = true)
    .schema(gameweekTestStruct)
    .csv("src/test/resources/gameweek_data.csv")

  val TEST_GAMEWEEK_DF_FORMATTED_DATE: DataFrame = TEST_GAMEWEEK_DF
    .withColumn(DATE_COL, to_date(col(UNFORMATTED_DATE_COL), DATE_FORMAT))
    .drop(UNFORMATTED_DATE_COL)

  val TEST_UNDERSTAT_DF: DataFrame = SPARK.read
    .option("header", value = true)
    .schema(understatTestStruct)
    .csv("src/test/resources/understat_data.csv")

  val TEST_UNDERSTAT_DF_FORMATTED_DATE: DataFrame = TEST_UNDERSTAT_DF
    .withColumn(DATE_COL, to_date(col(UNFORMATTED_DATE_COL), DATE_FORMAT))
    .drop(UNFORMATTED_DATE_COL)

  val EXPECTED_JOINED_FILTERED_DF: DataFrame = SPARK.read
    .option("header", value = true)
    .schema(joinedDataFilteredStruct)
    .csv("src/test/resources/joined_data_filtered.csv")

  val EXPECTED_JOINED_FILTERED_DF_FORMATTED_DATE: DataFrame = EXPECTED_JOINED_FILTERED_DF
    .withColumn(DATE_COL, to_date(col(UNFORMATTED_DATE_COL), DATE_FORMAT))
    .drop(UNFORMATTED_DATE_COL)
    .select("name", "date", "opponentTeam", "totalPoints", "xP", "team", "round", "position", "webName",
      "homeFixture", "month", "year", "bonusAvg", "cleanSheetsAvg", "goalsConcededAvg", "totalPointsAvg", "influenceAvg",
      "assistsAvg", "creativityAvg", "valueAvg", "goalsScoredAvg", "minutesAvg", "yellowCardsAvg", "threatAvg",
      "ictIndexAvg", "npxGAvg", "keyPassesAvg", "npgAvg", "xAAvg", "xGAvg", "shotsAvg", "xGBuildupAvg")

  test("getData - It should take 2 DataFrames, join them, calculate rolling averages, drop columns and drop null rows") {
    val joinedFilteredDf = new UnifiedDataProvider(TEST_GAMEWEEK_DF_FORMATTED_DATE, TEST_UNDERSTAT_DF_FORMATTED_DATE)
      .getData
    assert(EXPECTED_JOINED_FILTERED_DF_FORMATTED_DATE.schema === joinedFilteredDf.schema)
    assert(EXPECTED_JOINED_FILTERED_DF_FORMATTED_DATE.collect().sameElements(joinedFilteredDf.collect()))
  }
}
