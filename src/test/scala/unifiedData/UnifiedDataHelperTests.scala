package unifiedData

import com.github.mrpowers.spark.daria.sql.SparkSessionExt.SparkSessionMethods
import helpers.TestHelper
import helpers.schemas.JoinedDataAvgSchema.joinedDataAvgStruct
import helpers.schemas.JoinedDataDroppedSchema.joinedDataDroppedStruct
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType}
import unifiedData.UnifiedDataHelper.{dropColumnsAfterAvg, dropNullAvgs}

import java.sql.Date

class UnifiedDataHelperTests extends TestHelper {

  val DROPPED_COLUMNS: Seq[String] = Seq("bonus", "cleanSheets", "goalsConceded", "totalPoints", "teamAScore",
    "influence", "transfersIn", "saves", "assists", "creativity", "value", "selected", "goalsScored", "minutes",
    "yellowCards", "transfersOut", "threat", "ictIndex", "penaltiesSaved", "teamHScore", "npxG", "keyPasses", "npg",
    "xA", "xG", "shots", "xGBuildup")

  val TEST_JOINED_AVG_DF: DataFrame = SPARK.read
    .option("header", value = true)
    .schema(joinedDataAvgStruct)
    .csv("src/test/resources/joined_data_avg.csv")

  val EXPECTED_DROPPED_COLS_DF: DataFrame = SPARK.read
    .option("header", value = true)
    .schema(joinedDataDroppedStruct)
    .csv("src/test/resources/joined_data_dropped.csv")

  val TEST_NULL_AVGS_DF: DataFrame = SPARK.createDF(
    List(
      ("value1", null, null, Date.valueOf("2019-08-12")),
      ("value2", 1, 2.0, Date.valueOf("2019-08-11")),
      ("value3", 2, 4.0, Date.valueOf("2019-08-10"))
    ), List(
      ("col1", StringType, true),
      ("col2", IntegerType, true),
      ("bonusAvg", DoubleType, true),
      ("date", DateType, true)
    )
  )

  val EXPECTED_DROPPED_NULLS_DF: DataFrame = SPARK.createDF(
    List(
      ("value3", 2, 4.0, Date.valueOf("2019-08-10")),
      ("value2", 1, 2.0, Date.valueOf("2019-08-11"))
    ), List(
      ("col1", StringType, true),
      ("col2", IntegerType, true),
      ("bonusAvg", DoubleType, true),
      ("date", DateType, true)
    )
  )

  test("dropColumnsAfterAvg - It should return a DataFrame excluding columns that were to be dropped") {
    val dropColumnsAfterAvgDf: DataFrame = dropColumnsAfterAvg(TEST_JOINED_AVG_DF)
    val remainingColumns: Seq[String] = dropColumnsAfterAvgDf.columns.toSeq

    assert(EXPECTED_DROPPED_COLS_DF.schema === dropColumnsAfterAvgDf.schema)
    assert(EXPECTED_DROPPED_COLS_DF.collect().sameElements(dropColumnsAfterAvgDf.collect()))
    assert(DROPPED_COLUMNS.intersect(remainingColumns).isEmpty)
  }

  test("dropNullAvgs - It should return a DataFrame excluding rows with null values in the avg columns") {
    val dropNullAvgsDf: DataFrame = dropNullAvgs(TEST_NULL_AVGS_DF)
    assert(EXPECTED_DROPPED_NULLS_DF.schema === dropNullAvgsDf.schema)
    assert(EXPECTED_DROPPED_NULLS_DF.collect().sameElements(dropNullAvgsDf.collect()))
  }
}
