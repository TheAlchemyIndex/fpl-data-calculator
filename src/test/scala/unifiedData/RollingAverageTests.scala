package unifiedData

import com.github.mrpowers.spark.daria.sql.SparkSessionExt.SparkSessionMethods
import helpers.schemas.JoinedDataSchema.joinedDataStruct
import helpers.TestHelper
import helpers.schemas.JoinedDataAvgSchema.joinedDataAvgStruct
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, LongType, StringType}
import unifiedData.RollingAverage.{applyRollingAvg, calculateRollingAvg}

import java.sql.Date

class RollingAverageTests extends TestHelper {

  final val UNFORMATTED_DATE_COL = "unformattedDate"
  final val DATE_FORMAT = "dd/MM/yyyy"

  final val NAME_COL = "name"
  final val DATE_COL = "date"
  final val INTEGER_COL = "integerColumn"
  final val DOUBLE_COL = "doubleColumn"
  final val LONG_COL = "longColumn"
  final val INTEGER_AVG_COL = "integerColumnAvg"
  final val DOUBLE_AVG_COL = "doubleColumnAvg"
  final val LONG_AVG_COL = "longColumnAvg"
  final val NUM_OF_ROWS = 5

  val TEST_INTEGER_DF: DataFrame = SPARK.createDF(
    List(
      ("name1", Date.valueOf("2019-08-10"), 1),
      ("name1", Date.valueOf("2019-08-11"), 2),
      ("name1", Date.valueOf("2019-08-12"), 3),
      ("name1", Date.valueOf("2019-08-13"), 5),
      ("name1", Date.valueOf("2019-08-14"), 6),
      ("name1", Date.valueOf("2019-08-15"), 7),
      ("name1", Date.valueOf("2019-08-16"), 8),
      ("name2", Date.valueOf("2019-08-10"), 2),
      ("name2", Date.valueOf("2019-08-11"), 3),
      ("name2", Date.valueOf("2019-08-12"), 4),
      ("name2", Date.valueOf("2019-08-13"), 6),
      ("name2", Date.valueOf("2019-08-14"), 7),
      ("name2", Date.valueOf("2019-08-15"), 8),
      ("name2", Date.valueOf("2019-08-16"), 9)
    ), List(
      (NAME_COL, StringType, true),
      (DATE_COL, DateType, true),
      (INTEGER_COL, IntegerType, true)
    )
  )

  val EXPECTED_INTEGER_AVG_DF: DataFrame = SPARK.createDF(
    List(
      ("name1", Date.valueOf("2019-08-10"), 1, null),
      ("name1", Date.valueOf("2019-08-11"), 2, 1.0),
      ("name1", Date.valueOf("2019-08-12"), 3, 1.5),
      ("name1", Date.valueOf("2019-08-13"), 5, 2.0),
      ("name1", Date.valueOf("2019-08-14"), 6, 2.75),
      ("name1", Date.valueOf("2019-08-15"), 7, 3.4),
      ("name1", Date.valueOf("2019-08-16"), 8, 4.6),
      ("name2", Date.valueOf("2019-08-10"), 2, null),
      ("name2", Date.valueOf("2019-08-11"), 3, 2.0),
      ("name2", Date.valueOf("2019-08-12"), 4, 2.5),
      ("name2", Date.valueOf("2019-08-13"), 6, 3.0),
      ("name2", Date.valueOf("2019-08-14"), 7, 3.75),
      ("name2", Date.valueOf("2019-08-15"), 8, 4.4),
      ("name2", Date.valueOf("2019-08-16"), 9, 5.6)
    ), List(
      (NAME_COL, StringType, true),
      (DATE_COL, DateType, true),
      (INTEGER_COL, IntegerType, true),
      (INTEGER_AVG_COL, DoubleType, true)
    )
  )

  val TEST_DOUBLE_DF: DataFrame = SPARK.createDF(
    List(
      ("name1", Date.valueOf("2019-08-10"), 2.0),
      ("name1", Date.valueOf("2019-08-11"), 4.1),
      ("name1", Date.valueOf("2019-08-12"), 6.2),
      ("name1", Date.valueOf("2019-08-13"), 8.3),
      ("name1", Date.valueOf("2019-08-14"), 10.4),
      ("name1", Date.valueOf("2019-08-15"), 12.5),
      ("name1", Date.valueOf("2019-08-16"), 14.6),
      ("name2", Date.valueOf("2019-08-10"), 1.0),
      ("name2", Date.valueOf("2019-08-11"), 3.1),
      ("name2", Date.valueOf("2019-08-12"), 5.2),
      ("name2", Date.valueOf("2019-08-13"), 7.3),
      ("name2", Date.valueOf("2019-08-14"), 9.4),
      ("name2", Date.valueOf("2019-08-15"), 11.5),
      ("name2", Date.valueOf("2019-08-16"), 13.6)
    ), List(
      (NAME_COL, StringType, true),
      (DATE_COL, DateType, true),
      (DOUBLE_COL, DoubleType, true)
    )
  )

  val EXPECTED_DOUBLE_AVG_DF: DataFrame = SPARK.createDF(
    List(
      ("name1", Date.valueOf("2019-08-10"), 2.0, null),
      ("name1", Date.valueOf("2019-08-11"), 4.1, 2.0),
      ("name1", Date.valueOf("2019-08-12"), 6.2, 3.05),
      ("name1", Date.valueOf("2019-08-13"), 8.3, 4.10000),
      ("name1", Date.valueOf("2019-08-14"), 10.4, 5.15),
      ("name1", Date.valueOf("2019-08-15"), 12.5, 6.2),
      ("name1", Date.valueOf("2019-08-16"), 14.6, 8.3),
      ("name2", Date.valueOf("2019-08-10"), 1.0, null),
      ("name2", Date.valueOf("2019-08-11"), 3.1, 1.0),
      ("name2", Date.valueOf("2019-08-12"), 5.2, 2.05),
      ("name2", Date.valueOf("2019-08-13"), 7.3, 3.1),
      ("name2", Date.valueOf("2019-08-14"), 9.4, 4.15),
      ("name2", Date.valueOf("2019-08-15"), 11.5, 5.2),
      ("name2", Date.valueOf("2019-08-16"), 13.6, 7.3)
    ), List(
      (NAME_COL, StringType, true),
      (DATE_COL, DateType, true),
      (DOUBLE_COL, DoubleType, true),
      (DOUBLE_AVG_COL, DoubleType, true)
    )
  )

  val TEST_LONG_DF: DataFrame = SPARK.createDF(
    List(
      ("name1", Date.valueOf("2019-08-10"), 200L),
      ("name1", Date.valueOf("2019-08-11"), 410L),
      ("name1", Date.valueOf("2019-08-12"), 625L),
      ("name1", Date.valueOf("2019-08-13"), 845L),
      ("name1", Date.valueOf("2019-08-14"), 1060L),
      ("name1", Date.valueOf("2019-08-15"), 1275L),
      ("name1", Date.valueOf("2019-08-16"), 1490L),
      ("name2", Date.valueOf("2019-08-10"), 100L),
      ("name2", Date.valueOf("2019-08-11"), 305L),
      ("name2", Date.valueOf("2019-08-12"), 510L),
      ("name2", Date.valueOf("2019-08-13"), 715L),
      ("name2", Date.valueOf("2019-08-14"), 920L),
      ("name2", Date.valueOf("2019-08-15"), 1125L),
      ("name2", Date.valueOf("2019-08-16"), 1330L)
    ), List(
      (NAME_COL, StringType, true),
      (DATE_COL, DateType, true),
      (LONG_COL, LongType, true)
    )
  )

  val EXPECTED_LONG_AVG_DF: DataFrame = SPARK.createDF(
    List(
      ("name1", Date.valueOf("2019-08-10"), 200L, null),
      ("name1", Date.valueOf("2019-08-11"), 410L, 200.0),
      ("name1", Date.valueOf("2019-08-12"), 625L, 305.0),
      ("name1", Date.valueOf("2019-08-13"), 845L, 411.66667),
      ("name1", Date.valueOf("2019-08-14"), 1060L, 520.0),
      ("name1", Date.valueOf("2019-08-15"), 1275L, 628.0),
      ("name1", Date.valueOf("2019-08-16"), 1490L, 843.0),
      ("name2", Date.valueOf("2019-08-10"), 100L, null),
      ("name2", Date.valueOf("2019-08-11"), 305L, 100.0),
      ("name2", Date.valueOf("2019-08-12"), 510L, 202.5),
      ("name2", Date.valueOf("2019-08-13"), 715L, 305.0),
      ("name2", Date.valueOf("2019-08-14"), 920L, 407.5),
      ("name2", Date.valueOf("2019-08-15"), 1125L, 510.0),
      ("name2", Date.valueOf("2019-08-16"), 1330L, 715.0)
    ), List(
      (NAME_COL, StringType, true),
      (DATE_COL, DateType, true),
      (LONG_COL, LongType, true),
      (LONG_AVG_COL, DoubleType, true)
    )
  )

  val TEST_NULL_DF: DataFrame = SPARK.createDF(
    List(
      ("name1", Date.valueOf("2019-08-10"), null),
      ("name1", Date.valueOf("2019-08-11"), null),
      ("name1", Date.valueOf("2019-08-12"), null),
      ("name1", Date.valueOf("2019-08-13"), null),
      ("name1", Date.valueOf("2019-08-14"), null),
      ("name1", Date.valueOf("2019-08-15"), null),
      ("name2", Date.valueOf("2019-08-10"), null),
      ("name2", Date.valueOf("2019-08-11"), null),
      ("name2", Date.valueOf("2019-08-12"), null),
      ("name2", Date.valueOf("2019-08-13"), null),
      ("name2", Date.valueOf("2019-08-14"), null),
      ("name2", Date.valueOf("2019-08-15"), null),
    ), List(
      (NAME_COL, StringType, true),
      (DATE_COL, DateType, true),
      (INTEGER_COL, IntegerType, true)
    )
  )

  val EXPECTED_NULL_AVG_DF: DataFrame = SPARK.createDF(
    List(
      ("name1", Date.valueOf("2019-08-10"), null, null),
      ("name1", Date.valueOf("2019-08-11"), null, null),
      ("name1", Date.valueOf("2019-08-12"), null, null),
      ("name1", Date.valueOf("2019-08-13"), null, null),
      ("name1", Date.valueOf("2019-08-14"), null, null),
      ("name1", Date.valueOf("2019-08-15"), null, null),
      ("name2", Date.valueOf("2019-08-10"), null, null),
      ("name2", Date.valueOf("2019-08-11"), null, null),
      ("name2", Date.valueOf("2019-08-12"), null, null),
      ("name2", Date.valueOf("2019-08-13"), null, null),
      ("name2", Date.valueOf("2019-08-14"), null, null),
      ("name2", Date.valueOf("2019-08-15"), null, null),
    ), List(
      (NAME_COL, StringType, true),
      (DATE_COL, DateType, true),
      (INTEGER_COL, IntegerType, true),
      (INTEGER_AVG_COL, DoubleType, true)
    )
  )

  val TEST_JOINED_DF: DataFrame = SPARK.read
    .option("header", value = true)
    .schema(joinedDataStruct)
    .csv("src/test/resources/joined_data.csv")

  val TEST_JOINED_DF_FORMATTED_DATE: DataFrame = TEST_JOINED_DF
    .withColumn(DATE_COL, to_date(col(UNFORMATTED_DATE_COL), DATE_FORMAT))
    .drop(UNFORMATTED_DATE_COL)

  val EXPECTED_JOINED_AVG_DF: DataFrame = SPARK.read
    .option("header", value = true)
    .schema(joinedDataAvgStruct)
    .csv("src/test/resources/joined_data_avg.csv")

  val EXPECTED_JOINED_AVG_DF_FORMATTED_DATE: DataFrame = EXPECTED_JOINED_AVG_DF
    .withColumn(DATE_COL, to_date(col(UNFORMATTED_DATE_COL), DATE_FORMAT))
    .drop(UNFORMATTED_DATE_COL)
    .select("name", "opponentTeam", "bonus", "cleanSheets", "goalsConceded", "totalPoints", "teamAScore",
      "influence", "saves", "assists", "transfersIn", "xP", "creativity", "value", "selected", "goalsScored", "minutes",
      "yellowCards", "team", "transfersOut", "round", "position", "threat", "webName", "ictIndex", "penaltiesSaved",
      "teamHScore", "homeFixture", "month", "year", "npxG", "keyPasses", "npg", "xA", "xG", "shots", "xGBuildup",
      "date", "bonusAvg", "cleanSheetsAvg", "goalsConcededAvg", "totalPointsAvg", "influenceAvg", "assistsAvg",
      "creativityAvg", "valueAvg", "goalsScoredAvg", "minutesAvg", "yellowCardsAvg", "threatAvg", "ictIndexAvg",
      "npxGAvg", "keyPassesAvg", "npgAvg", "xAAvg", "xGAvg", "shotsAvg", "xGBuildupAvg")

  test("calculateRollingAvg - Integer - It should return a DataFrame containing a new column of rolling " +
    "averages for integerColumn") {
    val rollingAvgDf: DataFrame = calculateRollingAvg(TEST_INTEGER_DF, NAME_COL, INTEGER_COL, NUM_OF_ROWS)
    assert(EXPECTED_INTEGER_AVG_DF.schema === rollingAvgDf.schema)
    assert(EXPECTED_INTEGER_AVG_DF.collect().sameElements(rollingAvgDf.collect()))
  }

  test("calculateRollingAvg - Double - It should return a DataFrame containing a new column of rolling " +
    "averages for doubleColumn") {
    val rollingAvgDf: DataFrame = calculateRollingAvg(TEST_DOUBLE_DF, NAME_COL, DOUBLE_COL, NUM_OF_ROWS)
    assert(EXPECTED_DOUBLE_AVG_DF.schema === rollingAvgDf.schema)
    assert(EXPECTED_DOUBLE_AVG_DF.collect().sameElements(rollingAvgDf.collect()))
  }

  test("calculateRollingAvg - Long - It should return a DataFrame containing a new column of rolling " +
    "averages for longColumn") {
    val rollingAvgDf: DataFrame = calculateRollingAvg(TEST_LONG_DF, NAME_COL, LONG_COL, NUM_OF_ROWS)
    assert(EXPECTED_LONG_AVG_DF.schema === rollingAvgDf.schema)
    assert(EXPECTED_LONG_AVG_DF.collect().sameElements(rollingAvgDf.collect()))
  }

  test("calculateRollingAvg - null values - It should return a DataFrame containing a new column of rolling " +
    "averages of null values") {
    val rollingAvgDf: DataFrame = calculateRollingAvg(TEST_NULL_DF, NAME_COL, INTEGER_COL, NUM_OF_ROWS)
    assert(EXPECTED_NULL_AVG_DF.schema === rollingAvgDf.schema)
    assert(EXPECTED_NULL_AVG_DF.collect().sameElements(rollingAvgDf.collect()))
  }

  test("applyRollingAvg - It should return a DataFrame containing rolling averages for all relevant columns") {
    val applyRollingAvgDf: DataFrame = applyRollingAvg(TEST_JOINED_DF_FORMATTED_DATE, NUM_OF_ROWS)
    assert(EXPECTED_JOINED_AVG_DF_FORMATTED_DATE.schema === applyRollingAvgDf.schema)
    assert(EXPECTED_JOINED_AVG_DF_FORMATTED_DATE.collect().sameElements(applyRollingAvgDf.collect()))
  }
}
