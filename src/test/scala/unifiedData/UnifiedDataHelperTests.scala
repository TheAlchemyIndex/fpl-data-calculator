package unifiedData

import helpers.TestHelper
import helpers.schemas.JoinedDataAvgSchema.joinedDataAvgStruct
import helpers.schemas.JoinedDataDroppedSchema.joinedDataDroppedStruct
import org.apache.spark.sql.DataFrame
import unifiedData.UnifiedDataHelper.dropColumnsAfterAvg

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

  test("dropColumnsAfterAvg - It should return a DataFrame excluding columns that were to be dropped") {
    val dropColumnsAfterAvgDf: DataFrame = dropColumnsAfterAvg(TEST_JOINED_AVG_DF)
    val remainingColumns: Seq[String] = dropColumnsAfterAvgDf.columns.toSeq

    assert(EXPECTED_DROPPED_COLS_DF.schema === dropColumnsAfterAvgDf.schema)
    assert(EXPECTED_DROPPED_COLS_DF.collect().sameElements(dropColumnsAfterAvgDf.collect()))
    assert(DROPPED_COLUMNS.intersect(remainingColumns).isEmpty)
  }
}
