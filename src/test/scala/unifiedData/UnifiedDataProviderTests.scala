package unifiedData

import helpers.schemas.GameweekFilteredSchema.gameweekFilteredStruct
import helpers.schemas.JoinedDataSchema.joinedDataStruct
import helpers.TestHelper
import helpers.schemas.UnderstatFilteredSchema.understatFilteredStruct
import org.apache.spark.sql.DataFrame

class UnifiedDataProviderTests extends TestHelper {

  val TEST_GAMEWEEK_DF: DataFrame = SPARK.read
    .option("header", value = true)
    .schema(gameweekFilteredStruct)
    .csv("src/test/resources/gameweek_data.csv")

  val TEST_UNDERSTAT_DF: DataFrame = SPARK.read
    .option("header", value = true)
    .schema(understatFilteredStruct)
    .csv("src/test/resources/understat_data.csv")

  val EXPECTED_JOINED_DF: DataFrame = SPARK.read
    .option("header", value = true)
    .schema(joinedDataStruct)
    .csv("src/test/resources/joined_data.csv")

  val JOIN_COLUMNS: Seq[String] = Seq("name", "date")

  test("joinData - It should join 2 DataFrames together and return the resulting DataFrame") {
    val joinedDf = new UnifiedDataProvider(TEST_GAMEWEEK_DF, TEST_UNDERSTAT_DF).joinData(JOIN_COLUMNS)
    assert(EXPECTED_JOINED_DF.schema === joinedDf.schema)
    assert(EXPECTED_JOINED_DF.collect().sameElements(joinedDf.collect()))
  }

  test("getData") {
    val joinedDf = new UnifiedDataProvider(TEST_GAMEWEEK_DF, TEST_UNDERSTAT_DF).getData
  }
}
