package gameweek

import com.github.mrpowers.spark.daria.sql.SparkSessionExt.SparkSessionMethods
import helpers.TestHelper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{BooleanType, IntegerType, LongType, StringType, TimestampType}

import java.sql.Timestamp

class GameweekHelperTests extends TestHelper {

  final val GENERIC_COL: String = "col1"
  final val TRANSFERS_BALANCE_COL: String = "transfersBalance"
  final val OWN_GOALS_COL: String = "ownGoals"
  final val KICKOFF_COL: String = "kickoffTime"
  final val RED_CARDS_COL: String = "redCards"
  final val ELEMENT_COL: String = "element"
  final val BPS_COL: String = "bps"
  final val WAS_HOME_COL: String = "wasHome"
  final val PENS_MISSED_COL: String = "penaltiesMissed"
  final val FIXTURE_COL: String = "fixture"
  final val HOME_FIXTURE_COL: String = "homeFixture"

  val DROPPED_COLUMNS: Seq[String] = Seq(TRANSFERS_BALANCE_COL, OWN_GOALS_COL, KICKOFF_COL, RED_CARDS_COL, ELEMENT_COL,
    BPS_COL, WAS_HOME_COL, PENS_MISSED_COL, FIXTURE_COL)

  val TEST_BOOLEAN_DF: DataFrame = SPARK.createDF(
    List(
      ("value1", 100000L, 0, Timestamp.valueOf("2019-08-10 11:30:00"), 0, 100, 10, true, 0, 100),
      ("value2", 200000L, 0, Timestamp.valueOf("2020-09-11 12:00:00"), 0, 200, 20, false, 0, 200),
      ("value3", 300000L, 0, Timestamp.valueOf("2021-10-12 15:00:00"), 0, 300, 20, true, 0, 300)
    ), List(
      (GENERIC_COL, StringType, true),
      (TRANSFERS_BALANCE_COL, LongType, true),
      (OWN_GOALS_COL, IntegerType, true),
      (KICKOFF_COL, TimestampType, true),
      (RED_CARDS_COL, IntegerType, true),
      (ELEMENT_COL, IntegerType, true),
      (BPS_COL, IntegerType, true),
      (WAS_HOME_COL, BooleanType, true),
      (PENS_MISSED_COL, IntegerType, true),
      (FIXTURE_COL, IntegerType, true)
    )
  )

  val TEST_BOOLEAN_DF_NULL_VALUES: DataFrame = SPARK.createDF(
    List(
      ("value1", null),
      ("value2", null),
      ("value3", null)
    ), List(
      (GENERIC_COL, StringType, true),
      (WAS_HOME_COL, BooleanType, true)
    )
  )

  val EXPECTED_BINARY_DF: DataFrame = SPARK.createDF(
    List(
      ("value1", 100000L, 0, Timestamp.valueOf("2019-08-10 11:30:00"), 0, 100, 10, true, 0, 100, 1),
      ("value2", 200000L, 0, Timestamp.valueOf("2020-09-11 12:00:00"), 0, 200, 20, false, 0, 200, 0),
      ("value3", 300000L, 0, Timestamp.valueOf("2021-10-12 15:00:00"), 0, 300, 20, true, 0, 300, 1)
    ), List(
      (GENERIC_COL, StringType, true),
      (TRANSFERS_BALANCE_COL, LongType, true),
      (OWN_GOALS_COL, IntegerType, true),
      (KICKOFF_COL, TimestampType, true),
      (RED_CARDS_COL, IntegerType, true),
      (ELEMENT_COL, IntegerType, true),
      (BPS_COL, IntegerType, true),
      (WAS_HOME_COL, BooleanType, true),
      (PENS_MISSED_COL, IntegerType, true),
      (FIXTURE_COL, IntegerType, true),
      (HOME_FIXTURE_COL, IntegerType, true)
    )
  )

  val EXPECTED_BINARY_DF_NULL_VALUES: DataFrame = SPARK.createDF(
    List(
      ("value1", null, null),
      ("value2", null, null),
      ("value3", null, null)
    ), List(
      (GENERIC_COL, StringType, true),
      (WAS_HOME_COL, BooleanType, true),
      (HOME_FIXTURE_COL, IntegerType, true),
    )
  )

  val EXPECTED_BINARY_DF_DROPPED_COLUMNS: DataFrame = SPARK.createDF(
    List(
      "value1",
      "value2",
      "value3"
    ), List(
      (GENERIC_COL, StringType, true)
    )
  )
}
