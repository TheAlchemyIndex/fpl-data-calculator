package unifiedData

import com.github.mrpowers.spark.daria.sql.SparkSessionExt.SparkSessionMethods
import helpers.TestHelper
import helpers.schemas.joinedData.JoinedDataAvgTestSchema.joinedDataAvgTestStruct
import helpers.schemas.joinedData.JoinedDataDroppedTestSchema.joinedDataDroppedTestStruct
import helpers.schemas.joinedData.JoinedDataTestSchema.joinedDataTestStruct
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType}

import java.sql.Date

class UnifiedDataHelperTests extends TestHelper {

  final val DATE_COL: String = "date"
  final val DATE_FORMAT = "dd/MM/yyyy"
  final val NUM_OF_ROWS = 5
  val DROPPED_COLUMNS: Seq[String] = Seq("bonus", "cleanSheets", "goalsConceded", "teamAScore", "influence",
    "transfersIn", "saves", "assists", "creativity", "value", "selected", "goalsScored", "yellowCards", "transfersOut",
    "threat", "ictIndex", "penaltiesSaved", "teamHScore", "npxG", "keyPasses", "npg", "xA", "xG", "shots", "xGBuildup")

  val TEST_JOINED_DF: DataFrame = SPARK.read
    .option("header", value = true)
    .schema(joinedDataTestStruct)
    .csv("src/test/resources/joinedData/joined_data.csv")
    .withColumn(DATE_COL, to_date(col(DATE_COL), DATE_FORMAT))

  val EXPECTED_JOINED_AVG_DF: DataFrame = SPARK.read
    .option("header", value = true)
    .schema(joinedDataAvgTestStruct)
    .csv("src/test/resources/joinedData/joined_data_avg.csv")
    .withColumn(DATE_COL, to_date(col(DATE_COL), DATE_FORMAT))

  val EXPECTED_DROPPED_COLS_DF: DataFrame = SPARK.read
    .option("header", value = true)
    .schema(joinedDataDroppedTestStruct)
    .csv("src/test/resources/joinedData/joined_data_dropped.csv")
    .withColumn(DATE_COL, to_date(col(DATE_COL), DATE_FORMAT))

  val TEST_NULL_AVGS_DF: DataFrame = SPARK.createDF(
    List(
      ("value1", null, null, Date.valueOf("2019-08-10")),
      ("value2", null, 2.0, Date.valueOf("2019-08-11")),
      ("value3", 2, 4.0, Date.valueOf("2019-08-12"))
    ), List(
      ("col1", StringType, true),
      ("col2", IntegerType, true),
      ("bonusAvg", DoubleType, true),
      ("date", DateType, true)
    )
  )

  val EXPECTED_DROPPED_NULLS_DF: DataFrame = SPARK.createDF(
    List(
      ("value2", null, 2.0, Date.valueOf("2019-08-11")),
      ("value3", 2, 4.0, Date.valueOf("2019-08-12"))
    ), List(
      ("col1", StringType, true),
      ("col2", IntegerType, true),
      ("bonusAvg", DoubleType, true),
      ("date", DateType, true)
    )
  )
}
