package providers.impl.understat.players

import helpers.TestHelper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, to_date}
import providers.impl.understat.players.UnderstatPlayersProviderTestSchema.understatPlayersProviderTestStruct
import providers.impl.understat.players.UnderstatPlayersTestSchema.understatPlayersTestStruct

class UnderstatPlayersProviderTest extends TestHelper {

  final val DATE_COL: String = "date"
  final val DATE_FORMAT = "yyyy/dd/MM"
  final val DROPPED_COLUMNS: Seq[String] = Seq("x_g_chain", "h_goals", "a_team", "roster_id", "assists", "season",
    "a_goals", "time", "position", "id", "h_team", "goals")

  val TEST_UNDERSTAT_PLAYERS_DF: DataFrame = SPARK.read
    .option("header", value = true)
    .schema(understatPlayersTestStruct)
    .csv("src/test/resources/understat/understat_players_data.csv")
    .withColumn(DATE_COL, to_date(col(DATE_COL), DATE_FORMAT))

  val EXPECTED_UNDERSTAT_PLAYERS_PROVIDER_DF: DataFrame = SPARK.read
    .option("header", value = true)
    .schema(understatPlayersProviderTestStruct)
    .csv("src/test/resources/understat/understat_players_provider_data.csv")
    .withColumn(DATE_COL, to_date(col(DATE_COL), DATE_FORMAT))

  test("getData - It should return a DataFrame excluding columns that were to be dropped") {
    val understatPlayersProviderDf: DataFrame = new UnderstatPlayersProvider(TEST_UNDERSTAT_PLAYERS_DF).getData
    val remainingColumns: Seq[String] = understatPlayersProviderDf.columns.toSeq

    assert(EXPECTED_UNDERSTAT_PLAYERS_PROVIDER_DF.schema === understatPlayersProviderDf.schema)
    assert(EXPECTED_UNDERSTAT_PLAYERS_PROVIDER_DF.collect().sameElements(understatPlayersProviderDf.collect()))
    assert(DROPPED_COLUMNS.intersect(remainingColumns).isEmpty)
  }
}
