package providers.impl.gameweek

import helpers.TestHelper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, to_date}
import providers.impl.gameweek.GameweekProviderTestSchema.gameweekProviderTestStruct
import providers.impl.gameweek.GameweekTestSchema.gameweekTestStruct

class GameweekProviderTest extends TestHelper {

  final val DATE_COL: String = "date"
  final val DATE_FORMAT = "yyyy/dd/MM"
  final val DROPPED_COLUMNS: Seq[String] = Seq("transfers_balance", "own_goals", "kickoff_time", "red_cards", "team_a_score",
    "element", "bps", "was_home", "penalties_missed", "fixture", "team_h_score")

  final val TEST_GAMEWEEK_DF: DataFrame = SPARK.read
    .option("header", value = true)
    .schema(gameweekTestStruct)
    .csv("src/test/resources/gameweek/gameweek_data.csv")

  final val EXPECTED_GAMEWEEK_PROVIDER_DF: DataFrame = SPARK.read
    .option("header", value = true)
    .schema(gameweekProviderTestStruct)
    .csv("src/test/resources/gameweek/gameweek_provider_data.csv")
    .withColumn("date", to_date(col("date"), "dd/MM/yyyy"))


  test("getData - It should return a DataFrame containing a new homeFixture column with 1 or 0 values, " +
    "new date, month and year columns with date, month and year values and exclude columns that were to be dropped") {
    val gameweekProviderDf: DataFrame = new GameweekProvider(TEST_GAMEWEEK_DF).getData
    val gameweekProviderColumns: Seq[String] = gameweekProviderDf.columns.toSeq

    assert(EXPECTED_GAMEWEEK_PROVIDER_DF.schema === gameweekProviderDf.schema)
    assert(EXPECTED_GAMEWEEK_PROVIDER_DF.collect().sameElements(gameweekProviderDf.collect()))
    assert(DROPPED_COLUMNS.intersect(gameweekProviderColumns).isEmpty)
  }
}
