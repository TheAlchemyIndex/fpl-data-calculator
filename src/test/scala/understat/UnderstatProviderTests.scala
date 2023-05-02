package understat

import com.github.mrpowers.spark.daria.sql.SparkSessionExt.SparkSessionMethods
import helpers.TestHelper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType}

class UnderstatProviderTests extends TestHelper {

  final val GENERIC_COL: String = "col_num1"
  final val X_G_CHAIN_COL: String = "x_g_chain"
  final val H_GOALS_COL: String = "h_goals"
  final val A_TEAM_COL: String = "a_team"
  final val ROSTER_ID_COL: String = "roster_id"
  final val ASSISTS_COL: String = "assists"
  final val SEASON_COL: String = "season"
  final val A_GOALS_COL: String = "a_goals"
  final val TIME_COL: String = "time"
  final val POSITION_COL: String = "position"
  final val ID_COL: String = "id"
  final val H_TEAM_COL: String = "h_team"
  final val GOALS_COL: String = "goals"

  final var EXPECTED_CAMEL_CASE_COL: String = "colNum1"

  val DROPPED_COLUMNS: Seq[String] = Seq(X_G_CHAIN_COL, H_GOALS_COL, A_TEAM_COL, ROSTER_ID_COL, ASSISTS_COL, SEASON_COL,
    A_GOALS_COL, TIME_COL, POSITION_COL, ID_COL, H_TEAM_COL, GOALS_COL)

  val TEST_UNDERSTAT_DF: DataFrame = SPARK.createDF(
    List(
      ("value1", 0.123, 1, "Arsenal", 100000L, 1, 2019, 0, 90, "FW", 10000, "Chelsea", 1),
      ("value2", 0.456, 0, "Man Utd", 200000L, 0, 2020, 1, 45, "GK", 20000, "Fulham", 0),
      ("value3", 0.789, 2, "Liverpool", 300000L, 2, 2021, 2, 70, "Sub", 30000, "Leicester", 2),
    ), List(
      (GENERIC_COL, StringType, true),
      (X_G_CHAIN_COL, DoubleType, true),
      (H_GOALS_COL, IntegerType, true),
      (A_TEAM_COL, StringType, true),
      (ROSTER_ID_COL, LongType, true),
      (ASSISTS_COL, IntegerType, true),
      (SEASON_COL, IntegerType, true),
      (A_GOALS_COL, IntegerType, true),
      (TIME_COL, IntegerType, true),
      (POSITION_COL, StringType, true),
      (ID_COL, IntegerType, true),
      (H_TEAM_COL, StringType, true),
      (GOALS_COL, IntegerType, true)
    )
  )

  val EXPECTED_UNDERSTAT_FILTERED_DF: DataFrame = SPARK.createDF(
    List(
      "value1",
      "value2",
      "value3"
    ), List(
      (EXPECTED_CAMEL_CASE_COL, StringType, true)
    )
  )

  test("getData - It should return a DataFrame with column headers converted to camelCase and " +
    "exclude columns that were to be dropped") {
    val understatFilteredDf: DataFrame = new UnderstatProvider(TEST_UNDERSTAT_DF).getData
    val remainingColumns: Seq[String] = understatFilteredDf.columns.toSeq

    assert(EXPECTED_UNDERSTAT_FILTERED_DF.schema === understatFilteredDf.schema)
    assert(EXPECTED_UNDERSTAT_FILTERED_DF.collect().sameElements(understatFilteredDf.collect()))
    assert(DROPPED_COLUMNS.intersect(remainingColumns).isEmpty)
  }
}