package gameweek

import helpers.TestHelper
import helpers.schemas.gameweek.GameweekProviderTestSchema.gameweekProviderTestStruct
import helpers.schemas.gameweek.GameweekTestSchema.gameweekTestStruct
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, to_date}

class GameweekProviderTests extends TestHelper {

  final val DATE_COL: String = "date"
  final val DATE_FORMAT = "dd/MM/yyyy"
  val DROPPED_COLUMNS: Seq[String] = Seq("transfers_balance", "own_goals", "kickoff_time", "red_cards", "element",
    "bps", "was_home", "penalties_missed", "fixture")

  val TEST_GAMEWEEK_DF: DataFrame = SPARK.read
    .option("header", value = true)
    .schema(gameweekTestStruct)
    .csv("src/test/resources/gameweek/gameweek_data.csv")

  val EXPECTED_GAMEWEEK_PROVIDER_DF: DataFrame = SPARK.read
    .option("header", value = true)
    .schema(gameweekProviderTestStruct)
    .csv("src/test/resources/gameweek/gameweek_provider_data.csv")
    .withColumn(DATE_COL, to_date(col(DATE_COL), DATE_FORMAT))


  test("getData - It should return a DataFrame containing a new homeFixture column with 1 or 0 values, " +
    "new date, month and year columns with date, month and year values, column headers converted to camelCase and " +
    "exclude columns that were to be dropped") {
    val gameweekProviderDf: DataFrame = new GameweekProvider(TEST_GAMEWEEK_DF).getData
    val gameweekProviderColumns: Seq[String] = gameweekProviderDf.columns.toSeq

    assert(EXPECTED_GAMEWEEK_PROVIDER_DF.schema === gameweekProviderDf.schema)
    assert(EXPECTED_GAMEWEEK_PROVIDER_DF.collect().sameElements(gameweekProviderDf.collect()))
    assert(DROPPED_COLUMNS.intersect(gameweekProviderColumns).isEmpty)
  }
}
