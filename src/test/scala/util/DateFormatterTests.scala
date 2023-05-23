package util

import com.github.mrpowers.spark.daria.sql.SparkSessionExt.SparkSessionMethods
import helpers.TestHelper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, TimestampType}

import java.sql.{Date, Timestamp}


class DateFormatterTests extends TestHelper {

  final val GENERIC_COL: String = "col1"
  final val KICKOFF_COL: String = "kickoffTime"
  final val DATE_COL: String = "date"
  final val MONTH_COL: String = "month"
  final val YEAR_COL: String = "year"

  val TEST_TIMESTAMP_DF: DataFrame = SPARK.createDF(
    List(
      ("value1", Timestamp.valueOf("2019-08-10 11:30:00")),
      ("value2", Timestamp.valueOf("2020-09-11 12:00:00")),
      ("value3", Timestamp.valueOf("2021-10-12 15:00:00"))
    ), List(
      (GENERIC_COL, StringType, true),
      (KICKOFF_COL, TimestampType, true)
    )
  )

  val TEST_TIMESTAMP_DF_NULL_VALUES: DataFrame = SPARK.createDF(
    List(
      ("value1", null),
      ("value2", null),
      ("value3", null)
    ), List(
      (GENERIC_COL, StringType, true),
      (KICKOFF_COL, TimestampType, true)
    )
  )

  val EXPECTED_FORMATTED_DATE_DF: DataFrame = SPARK.createDF(
    List(
      ("value1", Timestamp.valueOf("2019-08-10 11:30:00"), Date.valueOf("2019-08-10"), 8, 2019),
      ("value2", Timestamp.valueOf("2020-09-11 12:00:00"), Date.valueOf("2020-09-11"), 9, 2020),
      ("value3", Timestamp.valueOf("2021-10-12 15:00:00"), Date.valueOf("2021-10-12"), 10, 2021)
    ), List(
      (GENERIC_COL, StringType, true),
      (KICKOFF_COL, TimestampType, true),
      (DATE_COL, DateType, true),
      (MONTH_COL, IntegerType, true),
      (YEAR_COL, IntegerType, true)
    )
  )

  val EXPECTED_FORMATTED_DATE_DF_NULL_VALUES: DataFrame = SPARK.createDF(
    List(
      ("value1", null, null, null, null),
      ("value2", null, null, null, null),
      ("value3", null, null, null, null)
    ), List(
      (GENERIC_COL, StringType, true),
      (KICKOFF_COL, TimestampType, true),
      (DATE_COL, DateType, true),
      (MONTH_COL, IntegerType, true),
      (YEAR_COL, IntegerType, true)
    )
  )
}
