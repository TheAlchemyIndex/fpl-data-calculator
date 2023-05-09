package gameweek

import com.github.mrpowers.spark.daria.sql.SparkSessionExt.SparkSessionMethods
import helpers.TestHelper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{BooleanType, DateType, IntegerType, LongType, StringType, TimestampType}

import java.sql.{Date, Timestamp}

class GameweekProviderTests extends TestHelper {

  final val GENERIC_COL: String = "col_num1"
  final val TRANSFERS_BALANCE_COL: String = "transfers_balance"
  final val OWN_GOALS_COL: String = "own_goals"
  final val KICKOFF_COL: String = "kickoff_time"
  final val RED_CARDS_COL: String = "red_cards"
  final val ELEMENT_COL: String = "element"
  final val BPS_COL: String = "bps"
  final val WAS_HOME_COL: String = "was_home"
  final val PENS_MISSED_COL: String = "penalties_missed"
  final val FIXTURE_COL: String = "fixture"
  final val HOME_FIXTURE_COL: String = "homeFixture"
  final val NAME_COL: String = "name"
  final val DATE_COL: String = "date"
  final val MONTH_COL: String = "month"
  final val YEAR_COL: String = "year"

  final var EXPECTED_CAMEL_CASE_COL: String = "colNum1"

  val DROPPED_COLUMNS: Seq[String] = Seq(TRANSFERS_BALANCE_COL, OWN_GOALS_COL, KICKOFF_COL, RED_CARDS_COL, ELEMENT_COL,
    BPS_COL, WAS_HOME_COL, PENS_MISSED_COL, FIXTURE_COL)

  val TEST_GAMEWEEK_DF: DataFrame = SPARK.createDF(
    List(
      ("value1", 100000L, 0, Timestamp.valueOf("2019-08-10 11:30:00"), 0, 100, 10, true, 0, "Test Name1", 100),
      ("value2", 200000L, 0, Timestamp.valueOf("2020-09-11 12:00:00"), 0, 200, 20, false, 0, "Test Name2", 200),
      ("value3", 300000L, 0, Timestamp.valueOf("2021-10-12 15:00:00"), 0, 300, 20, true, 0, "Test Name3", 300)
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
      (NAME_COL, StringType, true),
      (FIXTURE_COL, IntegerType, true)
    )
  )

  val TEST_GAMEWEEK_DF_NULL_VALUES: DataFrame = SPARK.createDF(
    List(
      ("value1", 100000L, 0, null, 0, 100, 10, null, 0, 100, null),
      ("value2", 200000L, 0, null, 0, 200, 20, null, 0, 200, null),
      ("value3", 300000L, 0, null, 0, 300, 20, null, 0, 300, null)
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
      (NAME_COL, StringType, true)
    )
  )

  val EXPECTED_GAMEWEEK_FILTERED_DF: DataFrame = SPARK.createDF(
    List(
      ("value1", "Test Name1", 1, Date.valueOf("2019-08-10"), 8, 2019, "Name1"),
      ("value2", "Test Name2", 0, Date.valueOf("2020-09-11"), 9, 2020, "Name2"),
      ("value3", "Test Name3", 1, Date.valueOf("2021-10-12"), 10, 2021, "Name3")
    ), List(
      (EXPECTED_CAMEL_CASE_COL, StringType, true),
      (NAME_COL, StringType, true),
      (HOME_FIXTURE_COL, IntegerType, false),
      (DATE_COL, DateType, true),
      (MONTH_COL, IntegerType, true),
      (YEAR_COL, IntegerType, true),
      ("surname", StringType, true)
    )
  )

  val EXPECTED_GAMEWEEK_FILTERED_DF_NULL_VALUES: DataFrame = SPARK.createDF(
    List(
      ("value1", null, 0, null, null, null, null),
      ("value2", null, 0, null, null, null, null),
      ("value3", null, 0, null, null, null, null)
    ), List(
      (EXPECTED_CAMEL_CASE_COL, StringType, true),
      (NAME_COL, StringType, true),
      (HOME_FIXTURE_COL, IntegerType, false),
      (DATE_COL, DateType, true),
      (MONTH_COL, IntegerType, true),
      (YEAR_COL, IntegerType, true),
      ("surname", StringType, true)
    )
  )

  test("getData - It should return a DataFrame containing a new homeFixture column with 1 or 0 values, " +
    "new date, month and year columns with date, month and year values, column headers converted to camelCase and " +
    "exclude columns that were to be dropped") {
    val gameweekFilteredDf: DataFrame = new GameweekProvider(TEST_GAMEWEEK_DF).getData
    val remainingColumns: Seq[String] = gameweekFilteredDf.columns.toSeq

    assert(EXPECTED_GAMEWEEK_FILTERED_DF.schema === gameweekFilteredDf.schema)
    assert(EXPECTED_GAMEWEEK_FILTERED_DF.collect().sameElements(gameweekFilteredDf.collect()))
    assert(DROPPED_COLUMNS.intersect(remainingColumns).isEmpty)
  }

  test("getData - null values - It should return a DataFrame containing a new homeFixture column with 0 " +
    "values, new date, month and year columns with null values, column headers converted to camelCase and exclude " +
    "columns that were to be dropped") {
    val gameweekFilteredDf: DataFrame = new GameweekProvider(TEST_GAMEWEEK_DF_NULL_VALUES).getData
    val remainingColumns: Seq[String] = gameweekFilteredDf.columns.toSeq

    assert(EXPECTED_GAMEWEEK_FILTERED_DF_NULL_VALUES.schema === gameweekFilteredDf.schema)
    assert(EXPECTED_GAMEWEEK_FILTERED_DF_NULL_VALUES.collect().sameElements(gameweekFilteredDf.collect()))
    assert(DROPPED_COLUMNS.intersect(remainingColumns).isEmpty)
  }
}
