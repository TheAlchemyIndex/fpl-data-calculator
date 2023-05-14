package unifiedData

import helpers.TestHelper
import helpers.schemas.unifiedData.UnifiedDataSchema.unifiedDataStruct
import helpers.schemas.gameweek.GameweekProviderTestSchema.gameweekProviderTestStruct
import helpers.schemas.understat.UnderstatTestSchema.understatTestStruct
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, to_date}

class UnifiedDataProviderTests extends TestHelper {

  final val DATE_FORMAT = "dd/MM/yyyy"
  final val DATE_COL = "date"
  final val NAME_COL = "name"

  val TEST_GAMEWEEK_DF: DataFrame = SPARK.read
    .option("header", value = true)
    .schema(gameweekProviderTestStruct)
    .csv("src/test/resources/gameweek/gameweek_provider_data.csv")
    .withColumn(DATE_COL, to_date(col(DATE_COL), DATE_FORMAT))

  val TEST_UNDERSTAT_DF: DataFrame = SPARK.read
    .option("header", value = true)
    .schema(understatTestStruct)
    .csv("src/test/resources/understat/understat_data.csv")
    .withColumn(DATE_COL, to_date(col(DATE_COL), DATE_FORMAT))

  val EXPECTED_UNIFIED_DATA_DF: DataFrame = SPARK.read
    .option("header", value = true)
    .schema(unifiedDataStruct)
    .csv("src/test/resources/unifiedData/unified_data.csv")
    .withColumn(DATE_COL, to_date(col(DATE_COL), DATE_FORMAT))

  test("getData - It should take 2 DataFrames, join them, calculate rolling averages, drop columns and drop null rows") {
    val unifiedDataDf = new UnifiedDataProvider(TEST_GAMEWEEK_DF, TEST_UNDERSTAT_DF).getData
    assert(EXPECTED_UNIFIED_DATA_DF.collect().sameElements(unifiedDataDf.collect()))
  }
}
