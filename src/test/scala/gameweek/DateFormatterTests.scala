package gameweek

import com.github.mrpowers.spark.daria.sql.SparkSessionExt.SparkSessionMethods
import gameweek.DateFormatter.{dateToMonthAndYear, formatDate, timestampToDate}
import helpers.TestHelper
import org.apache.spark.sql.DataFrame

import java.sql.{Date, Timestamp}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, TimestampType}


class DateFormatterTests extends TestHelper {

  val TEST_TIMESTAMP_DF: DataFrame = SPARK.createDF(
    List(
      (Timestamp.valueOf("2019-08-10 11:30:00"), "value1"),
      (Timestamp.valueOf("2020-09-11 12:00:00"), "value2"),
      (Timestamp.valueOf("2021-10-12 15:00:00"), "value3")
    ), List(
      ("kickoffTime", TimestampType, true),
      ("col1", StringType, true)
    )
  )

  val TEST_TIMESTAMP_DF_NULL_VALUES: DataFrame = SPARK.createDF(
    List(
      (null, "value1"),
      (null, "value2"),
      (null, "value3")
    ), List(
      ("kickoffTime", TimestampType, true),
      ("col1", StringType, true)
    )
  )

  val TEST_DATE_DF: DataFrame = SPARK.createDF(
    List(
      (Date.valueOf("2019-08-10"), "value1"),
      (Date.valueOf("2020-09-11"), "value2"),
      (Date.valueOf("2021-10-12"), "value3")
    ), List(
      ("date", DateType, true),
      ("col1", StringType, true)
    )
  )

  val TEST_DATE_DF_NULL_VALUES: DataFrame = SPARK.createDF(
    List(
      (null, "value1"),
      (null, "value2"),
      (null, "value3")
    ), List(
      ("date", DateType, true),
      ("col1", StringType, true)
    )
  )

  val EXPECTED_TIMESTAMP_DF: DataFrame = SPARK.createDF(
    List(
      (Timestamp.valueOf("2019-08-10 11:30:00"), "value1", Date.valueOf("2019-08-10")),
      (Timestamp.valueOf("2020-09-11 12:00:00"), "value2", Date.valueOf("2020-09-11")),
      (Timestamp.valueOf("2021-10-12 15:00:00"), "value3", Date.valueOf("2021-10-12"))
    ), List(
      ("kickoffTime", TimestampType, true),
      ("col1", StringType, true),
      ("date", DateType, true)
    )
  )

  val EXPECTED_TIMESTAMP_DF_NULL_VALUES: DataFrame = SPARK.createDF(
    List(
      (null, "value1", null),
      (null, "value2", null),
      (null, "value3", null)
    ), List(
      ("kickoffTime", TimestampType, true),
      ("col1", StringType, true),
      ("date", DateType, true)
    )
  )

  val EXPECTED_DATE_DF: DataFrame = SPARK.createDF(
    List(
      (Date.valueOf("2019-08-10"), "value1", 8, 2019),
      (Date.valueOf("2020-09-11"), "value2", 9, 2020),
      (Date.valueOf("2021-10-12"), "value3", 10, 2021)
    ), List(
      ("date", DateType, true),
      ("col1", StringType, true),
      ("month", IntegerType, true),
      ("year", IntegerType, true)
    )
  )

  val EXPECTED_DATE_DF_NULL_VALUES: DataFrame = SPARK.createDF(
    List(
      (null, "value1", null, null),
      (null, "value2", null, null),
      (null, "value3", null, null)
    ), List(
      ("date", DateType, true),
      ("col1", StringType, true),
      ("month", IntegerType, true),
      ("year", IntegerType, true)
    )
  )

  val EXPECTED_FORMATTED_DATE_DF: DataFrame = SPARK.createDF(
    List(
      (Timestamp.valueOf("2019-08-10 11:30:00"), "value1", Date.valueOf("2019-08-10"), 8, 2019),
      (Timestamp.valueOf("2020-09-11 12:00:00"), "value2", Date.valueOf("2020-09-11"), 9, 2020),
      (Timestamp.valueOf("2021-10-12 15:00:00"), "value3", Date.valueOf("2021-10-12"), 10, 2021)
    ), List(
      ("kickoffTime", TimestampType, true),
      ("col1", StringType, true),
      ("date", DateType, true),
      ("month", IntegerType, true),
      ("year", IntegerType, true)
    )
  )

  val EXPECTED_FORMATTED_DATE_DF_NULL_VALUES: DataFrame = SPARK.createDF(
    List(
      (null, "value1", null, null, null),
      (null, "value2", null, null, null),
      (null, "value3", null, null, null)
    ), List(
      ("kickoffTime", TimestampType, true),
      ("col1", StringType, true),
      ("date", DateType, true),
      ("month", IntegerType, true),
      ("year", IntegerType, true)
    )
  )

  test("timestampToDate - It should return a DataFrame containing a new date column with Date values") {
    val dateDf = timestampToDate(TEST_TIMESTAMP_DF)
    assert(EXPECTED_TIMESTAMP_DF.schema === dateDf.schema)
    assert(EXPECTED_TIMESTAMP_DF.collect().sameElements(dateDf.collect()))
  }

  test("timestampToDate - null values - It should return a DataFrame containing a new date column with null values") {
    val dateDf = timestampToDate(TEST_TIMESTAMP_DF_NULL_VALUES)
    assert(EXPECTED_TIMESTAMP_DF_NULL_VALUES.schema === dateDf.schema)
    assert(EXPECTED_TIMESTAMP_DF_NULL_VALUES.collect().sameElements(dateDf.collect()))
  }

  test("dateToMonthAndYear - It should return a DataFrame containing new month and year columns with month and year values") {
    val monthYearDf = dateToMonthAndYear(TEST_DATE_DF)
    assert(EXPECTED_DATE_DF.schema === monthYearDf.schema)
    assert(EXPECTED_DATE_DF.collect().sameElements(monthYearDf.collect()))
  }

  test("dateToMonthAndYear - null values - It should return a DataFrame containing new month and year columns with null values") {
    val monthYearDf = dateToMonthAndYear(TEST_DATE_DF_NULL_VALUES)
    assert(EXPECTED_DATE_DF_NULL_VALUES.schema === monthYearDf.schema)
    assert(EXPECTED_DATE_DF_NULL_VALUES.collect().sameElements(monthYearDf.collect()))
  }

  test("formatDate - It should return a DataFrame containing new date, month and year columns with date, month and year values") {
    val formatDateDf = formatDate(TEST_TIMESTAMP_DF)
    assert(EXPECTED_FORMATTED_DATE_DF.schema === formatDateDf.schema)
    assert(EXPECTED_FORMATTED_DATE_DF.collect().sameElements(formatDateDf.collect()))
  }

  test("formatDate - null values - It should return a DataFrame containing new date, month and year columns with null values") {
    val formatDateDf = formatDate(TEST_TIMESTAMP_DF)
    assert(EXPECTED_FORMATTED_DATE_DF.schema === formatDateDf.schema)
    assert(EXPECTED_FORMATTED_DATE_DF.collect().sameElements(formatDateDf.collect()))
  }
}