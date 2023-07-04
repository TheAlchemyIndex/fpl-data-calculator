package providers.impl.fixtures

import helpers.TestHelper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, to_date}
import providers.impl.fixtures.FixturesProviderTestSchema.fixturesProviderTestStruct
import providers.impl.fixtures.FixturesTestSchema.fixturesTestStruct

class FixturesProviderTest extends TestHelper {

  final val DROPPED_COLUMNS: Seq[String] = Seq("code", "provisional_start_time", "kickoff_time", "minutes", "finished",
    "started", "finished_provisional", "stats", "id", "event")

  final val TEST_FIXTURES_DF: DataFrame = SPARK.read
    .option("header", value = true)
    .schema(fixturesTestStruct)
    .csv("src/test/resources/fixtures/fixtures_data.csv")

  final val EXPECTED_FIXTURES_PROVIDER_DF: DataFrame = SPARK.read
    .option("header", value = true)
    .schema(fixturesProviderTestStruct)
    .csv("src/test/resources/fixtures/fixtures_provider_data.csv")
    .withColumn("date", to_date(col("date"), "dd/MM/yyyy"))

  test("getData - It should return a fixtures DataFrame with a date formatted column and exclude columns that " +
    "were to be dropped") {
    val fixturesProviderDf: DataFrame = new FixturesProvider(TEST_FIXTURES_DF).getData
    val fixturesProviderColumns: Seq[String] = fixturesProviderDf.columns.toSeq

    assert(EXPECTED_FIXTURES_PROVIDER_DF.schema === fixturesProviderDf.schema)
    assert(EXPECTED_FIXTURES_PROVIDER_DF.collect().sameElements(fixturesProviderDf.collect()))
    assert(DROPPED_COLUMNS.intersect(fixturesProviderColumns).isEmpty)
  }
}
