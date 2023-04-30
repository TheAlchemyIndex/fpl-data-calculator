package util

import com.github.mrpowers.spark.daria.sql.SparkSessionExt.SparkSessionMethods
import helpers.TestHelper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType
import util.DataFrameHelper.{fromSnakeCaseToCamelCase, renameColumns}

class DataFrameHelperTests extends TestHelper {

  val COL1_SNAKE_CASE = "col_num1"
  val COL2_SNAKE_CASE = "col_num2"
  val COL3_SNAKE_CASE = "col_num3"
  val SNAKE_CASE_COLS: Array[String] = Array(COL1_SNAKE_CASE, COL2_SNAKE_CASE, COL3_SNAKE_CASE)

  val COL1_CAMEL_CASE = "colNum1"
  val COL2_CAMEL_CASE = "colNum2"
  val COL3_CAMEL_CASE = "colNum3"
  val CAMEL_CASE_COLS: Array[String] = Array(COL1_CAMEL_CASE, COL2_CAMEL_CASE, COL3_CAMEL_CASE)

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

  test("fromSnakeCaseToCamelCase - It should take an Array of snake case column names and return an Array" +
    "of camel case column names") {
    assert(EXPECTED_CAMEL_CASE_COLS.sameElements(fromSnakeCaseToCamelCase(SNAKE_CASE_COLS)))
  }

  test("fromSnakeCaseToCamelCase - It should take an Array of camel case column names and return an Array" +
    "of the same column names") {
    assert(EXPECTED_CAMEL_CASE_COLS.sameElements(fromSnakeCaseToCamelCase(CAMEL_CASE_COLS)))
  }

  test("renameColumns - It should return a DataFrame with snake case column headers converted to camelCase") {
    val camelCaseDf: DataFrame = renameColumns(TEST_SNAKE_CASE_DF)
    assert(EXPECTED_CAMEL_CASE_DF.schema === camelCaseDf.schema)
    assert(EXPECTED_CAMEL_CASE_DF.collect().sameElements(camelCaseDf.collect()))
  }

  test("renameColumns - It should return a DataFrame with camel case column headers unchanged") {
    val camelCaseDf: DataFrame = renameColumns(TEST_CAMEL_CASE_DF)
    assert(EXPECTED_CAMEL_CASE_DF.schema === camelCaseDf.schema)
    assert(EXPECTED_CAMEL_CASE_DF.collect().sameElements(camelCaseDf.collect()))
  }
}
