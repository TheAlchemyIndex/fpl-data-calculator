package util

import com.github.mrpowers.spark.daria.sql.SparkSessionExt.SparkSessionMethods
import helpers.TestHelper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType}
import util.DataFrameHelper.{booleanColumnToBinary, dropNullRows, joinDataLeftOuter, renameColumnsToCamelCase}

class DataFrameHelperTests extends TestHelper {

  final val COL1_SNAKE_CASE = "col_num1"
  final val COL2_SNAKE_CASE = "col_num2"
  final val COL3_SNAKE_CASE = "col_num3"
  final val COL1_CAMEL_CASE = "colNum1"
  final val COL2_CAMEL_CASE = "colNum2"
  final val COL3_CAMEL_CASE = "colNum3"
  final val WAS_HOME_COL: String = "wasHome"
  final val HOME_FIXTURE_COL: String = "homeFixture"
  val EXPECTED_CAMEL_CASE_COLS: Array[String] = Array("colNum1", "colNum2", "colNum3")

  val TEST_SNAKE_CASE_DF: DataFrame = SPARK.createDF(
    List(
      ("value1", "value2", "value3"),
      ("value1", "value2", "value3"),
      ("value1", "value2", "value3")
    ), List(
      (COL1_SNAKE_CASE, StringType, true),
      (COL2_SNAKE_CASE, StringType, true),
      (COL3_SNAKE_CASE, StringType, true)
    )
  )

  val TEST_CAMEL_CASE_DF: DataFrame = SPARK.createDF(
    List(
      ("value1", "value2", "value3"),
      ("value1", "value2", "value3"),
      ("value1", "value2", "value3")
    ), List(
      (COL1_CAMEL_CASE, StringType, true),
      (COL2_CAMEL_CASE, StringType, true),
      (COL3_CAMEL_CASE, StringType, true)
    )
  )

  val EXPECTED_CAMEL_CASE_DF: DataFrame = SPARK.createDF(
    List(
      ("value1", "value2", "value3"),
      ("value1", "value2", "value3"),
      ("value1", "value2", "value3")
    ), List(
      (COL1_CAMEL_CASE, StringType, true),
      (COL2_CAMEL_CASE, StringType, true),
      (COL3_CAMEL_CASE, StringType, true)
    )
  )

  val TEST_BOOLEAN_DF: DataFrame = SPARK.createDF(
    List(true, false, true),
    List((WAS_HOME_COL, BooleanType, true))
  )

  val TEST_BOOLEAN_DF_NULL_VALUES: DataFrame = SPARK.createDF(
    List(null, null, null),
    List((WAS_HOME_COL, BooleanType, true))
  )

  val EXPECTED_BINARY_DF: DataFrame = SPARK.createDF(
    List(
      (true, 1),
      (false, 0),
      (true, 1)
    ), List(
      (WAS_HOME_COL, BooleanType, true),
      (HOME_FIXTURE_COL, IntegerType, true),
    )
  )

  val EXPECTED_BINARY_DF_NULL_VALUES: DataFrame = SPARK.createDF(
    List(
      (null, null),
      (null, null),
      (null, null)
    ), List(
      (WAS_HOME_COL, BooleanType, true),
      (HOME_FIXTURE_COL, IntegerType, true),
    )
  )

  val TEST_NULL_ROWS_DF: DataFrame = SPARK.createDF(
    List(
      ("value1", null, "value3"),
      ("value1", "value2", "value3"),
      ("value1", null, "value3")
    ), List(
      (COL1_CAMEL_CASE, StringType, true),
      (COL2_CAMEL_CASE, StringType, true),
      (COL3_CAMEL_CASE, StringType, true)
    )
  )

  val EXPECTED_NULL_ROWS_DROPPED_DF: DataFrame = SPARK.createDF(
    List(
      ("value1", "value2", "value3"),
    ), List(
      (COL1_CAMEL_CASE, StringType, true),
      (COL2_CAMEL_CASE, StringType, true),
      (COL3_CAMEL_CASE, StringType, true)
    )
  )

  val TEST_LEFT_SINGLE_DF: DataFrame = SPARK.createDF(
    List(
      ("value1", "value10"),
      ("value2", "value10"),
      ("value3", "value10")
    ), List(
      (COL1_CAMEL_CASE, StringType, true),
      (COL2_CAMEL_CASE, StringType, true)
    )
  )

  val TEST_RIGHT_SINGLE_DF: DataFrame = SPARK.createDF(
    List(
      ("value1", "value20"),
      ("value2", "value20"),
      ("value3", "value20")
    ), List(
      (COL1_CAMEL_CASE, StringType, true),
      (COL3_CAMEL_CASE, StringType, true)
    )
  )

  val EXPECTED_JOINED_SINGLE_DF: DataFrame = SPARK.createDF(
    List(
      ("value1", "value10", "value20"),
      ("value2", "value10", "value20"),
      ("value3", "value10", "value20")
    ), List(
      (COL1_CAMEL_CASE, StringType, true),
      (COL2_CAMEL_CASE, StringType, true),
      (COL3_CAMEL_CASE, StringType, true)
    )
  )

  val TEST_LEFT_DOUBLE_DF: DataFrame = SPARK.createDF(
    List(
      ("value1", "value10", "value40"),
      ("value2", "value20", "value50"),
      ("value3", "value30", "value60")
    ), List(
      (WAS_HOME_COL, StringType, true),
      (COL1_CAMEL_CASE, StringType, true),
      (COL2_CAMEL_CASE, StringType, true)
    )
  )

  val TEST_RIGHT_DOUBLE_DF: DataFrame = SPARK.createDF(
    List(
      ("value1", "value10", "value70"),
      ("value2", "value20", "value80"),
      ("value3", "value30", "value90")
    ), List(
      (WAS_HOME_COL, StringType, true),
      (COL1_CAMEL_CASE, StringType, true),
      (COL3_CAMEL_CASE, StringType, true)
    )
  )

  val EXPECTED_JOINED_DOUBLE_DF: DataFrame = SPARK.createDF(
    List(
      ("value1", "value10", "value40", "value70"),
      ("value2", "value20", "value50", "value80"),
      ("value3", "value30", "value60", "value90")
    ), List(
      (WAS_HOME_COL, StringType, true),
      (COL1_CAMEL_CASE, StringType, true),
      (COL2_CAMEL_CASE, StringType, true),
      (COL3_CAMEL_CASE, StringType, true)
    )
  )

  test("renameColumns - It should return a DataFrame with snake case column headers converted to camelCase") {
    val camelCaseDf: DataFrame = renameColumnsToCamelCase(TEST_SNAKE_CASE_DF)
    assert(EXPECTED_CAMEL_CASE_DF.schema === camelCaseDf.schema)
    assert(EXPECTED_CAMEL_CASE_DF.collect().sameElements(camelCaseDf.collect()))
  }

  test("renameColumns - It should return a DataFrame with camel case column headers unchanged") {
    val camelCaseDf: DataFrame = renameColumnsToCamelCase(TEST_CAMEL_CASE_DF)
    assert(EXPECTED_CAMEL_CASE_DF.schema === camelCaseDf.schema)
    assert(EXPECTED_CAMEL_CASE_DF.collect().sameElements(camelCaseDf.collect()))
  }

  test("booleanColumnToBinary - It should return a DataFrame containing a new homeFixture column with 1 or 0 values") {
    val binaryDf: DataFrame = booleanColumnToBinary(TEST_BOOLEAN_DF, HOME_FIXTURE_COL, WAS_HOME_COL)
    assert(EXPECTED_BINARY_DF.schema === binaryDf.schema)
    assert(EXPECTED_BINARY_DF.collect().sameElements(binaryDf.collect()))
  }

  test("booleanColumnToBinary - null values - It should return a DataFrame containing a new homeFixture column with 0 values") {
    val binaryDf: DataFrame = booleanColumnToBinary(TEST_BOOLEAN_DF_NULL_VALUES, HOME_FIXTURE_COL, WAS_HOME_COL)
    assert(EXPECTED_BINARY_DF_NULL_VALUES.schema === binaryDf.schema)
    assert(EXPECTED_BINARY_DF_NULL_VALUES.collect().sameElements(binaryDf.collect()))
  }

  test("dropNullRows - It should return a DataFrame containing no rows with null values") {
    val droppedNullRowsDf: DataFrame = dropNullRows(TEST_NULL_ROWS_DF, Seq(COL2_CAMEL_CASE))
    assert(EXPECTED_NULL_ROWS_DROPPED_DF.schema === droppedNullRowsDf.schema)
    assert(EXPECTED_NULL_ROWS_DROPPED_DF.collect().sameElements(droppedNullRowsDf.collect()))
  }

  test("joinDataLeftOuter - Single Column - It should join 2 DataFrames together on a single column and return the joined DataFrame") {
    val joinedDf: DataFrame = joinDataLeftOuter(TEST_LEFT_SINGLE_DF, TEST_RIGHT_SINGLE_DF, Seq(COL1_CAMEL_CASE))
    assert(EXPECTED_JOINED_SINGLE_DF.schema === joinedDf.schema)
    assert(EXPECTED_JOINED_SINGLE_DF.collect().sameElements(joinedDf.collect()))
  }

  test("joinDataLeftOuter - Two Columns - It should join 2 DataFrames together on 2 columns and return the joined DataFrame") {
    val joinedDf: DataFrame = joinDataLeftOuter(TEST_LEFT_DOUBLE_DF, TEST_RIGHT_DOUBLE_DF, Seq(WAS_HOME_COL, COL1_CAMEL_CASE))
    assert(EXPECTED_JOINED_DOUBLE_DF.schema === joinedDf.schema)
    assert(EXPECTED_JOINED_DOUBLE_DF.collect().sameElements(joinedDf.collect()))
  }
}
