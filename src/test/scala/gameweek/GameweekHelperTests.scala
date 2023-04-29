package gameweek

import com.github.mrpowers.spark.daria.sql.SparkSessionExt.SparkSessionMethods
import gameweek.GameweekHelper.{booleanColumnToBinary, dropColumns}
import helpers.TestHelper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{BooleanType, IntegerType, LongType, StringType, TimestampType}

import java.sql.Timestamp

class GameweekHelperTests extends TestHelper {

  val TEST_BOOLEAN_DF: DataFrame = SPARK.createDF(
    List(
      ("value1", 100000L, 0, Timestamp.valueOf("2019-08-10 11:30:00"), 0, 100, 10, true, 0, 100),
      ("value2", 200000L, 0, Timestamp.valueOf("2020-09-11 12:00:00"), 0, 200, 20, false, 0, 200),
      ("value3", 300000L, 0, Timestamp.valueOf("2021-10-12 15:00:00"), 0, 300, 20, true, 0, 300)
    ), List(
      ("col1", StringType, true),
      ("transfersBalance", LongType, true),
      ("ownGoals", IntegerType, true),
      ("kickoffTime", TimestampType, true),
      ("redCards", IntegerType, true),
      ("element", IntegerType, true),
      ("bps", IntegerType, true),
      ("wasHome", BooleanType, true),
      ("penaltiesMissed", IntegerType, true),
      ("fixture", IntegerType, true)
    )
  )

  val TEST_BOOLEAN_DF_NULL_VALUES: DataFrame = SPARK.createDF(
    List(
      (null, "value1"),
      (null, "value2"),
      (null, "value3")
    ), List(
      ("wasHome", BooleanType, true),
      ("col1", StringType, true)
    )
  )

  val EXPECTED_BINARY_DF: DataFrame = SPARK.createDF(
    List(
      ("value1", 100000L, 0, Timestamp.valueOf("2019-08-10 11:30:00"), 0, 100, 10, true, 0, 100, 1),
      ("value2", 200000L, 0, Timestamp.valueOf("2020-09-11 12:00:00"), 0, 200, 20, false, 0, 200, 0),
      ("value3", 300000L, 0, Timestamp.valueOf("2021-10-12 15:00:00"), 0, 300, 20, true, 0, 300, 1)
    ), List(
      ("col1", StringType, true),
      ("transfersBalance", LongType, true),
      ("ownGoals", IntegerType, true),
      ("kickoffTime", TimestampType, true),
      ("redCards", IntegerType, true),
      ("element", IntegerType, true),
      ("bps", IntegerType, true),
      ("wasHome", BooleanType, true),
      ("penaltiesMissed", IntegerType, true),
      ("fixture", IntegerType, true),
      ("homeFixture", IntegerType, false)
    )
  )

  val EXPECTED_BINARY_DF_NULL_VALUES: DataFrame = SPARK.createDF(
    List(
      (null, "value1", 0),
      (null, "value2", 0),
      (null, "value3", 0)
    ), List(
      ("wasHome", BooleanType, true),
      ("col1", StringType, true),
      ("homeFixture", IntegerType, false),
    )
  )

  val EXPECTED_BINARY_DF_DROPPED_COLUMNS: DataFrame = SPARK.createDF(
    List(
      "value1",
      "value2",
      "value3"
    ), List(
      ("col1", StringType, true)
    )
  )

  val DROPPED_COLUMNS: Seq[String] = Seq("transfersBalance", "ownGoals", "kickoffTime", "redCards", "element", "bps", "wasHome", "penaltiesMissed", "fixture")

  test("booleanColumnToBinary - It should return a DataFrame containing a new homeFixture column with 1 or 0 values") {
    val binaryDf = booleanColumnToBinary(TEST_BOOLEAN_DF, "homeFixture", "wasHome")
    assert(EXPECTED_BINARY_DF.schema === binaryDf.schema)
    assert(EXPECTED_BINARY_DF.collect().sameElements(binaryDf.collect()))
  }

  test("booleanColumnToBinary - null values - It should return a DataFrame containing a new homeFixture column with 0 values") {
    val binaryDf = booleanColumnToBinary(TEST_BOOLEAN_DF_NULL_VALUES, "homeFixture", "wasHome")
    assert(EXPECTED_BINARY_DF_NULL_VALUES.schema === binaryDf.schema)
    assert(EXPECTED_BINARY_DF_NULL_VALUES.collect().sameElements(binaryDf.collect()))
  }

  test("dropColumns - It should return a DataFrame excluding columns that were to be dropped") {
    val droppedColumnsDf = dropColumns(TEST_BOOLEAN_DF)
    val remainingColumns = droppedColumnsDf.columns.toSeq

    assert(EXPECTED_BINARY_DF_DROPPED_COLUMNS.schema === droppedColumnsDf.schema)
    assert(EXPECTED_BINARY_DF_DROPPED_COLUMNS.collect().sameElements(droppedColumnsDf.collect()))
    assert(DROPPED_COLUMNS.intersect(remainingColumns).isEmpty)
  }
}
