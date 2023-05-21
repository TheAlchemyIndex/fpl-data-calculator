import org.apache.spark.sql.{DataFrame, SparkSession}
import providers.fixtures.FixturesProvider
import providers.fixtures.FixturesSchema.fixturesStruct
import providers.gameweek.GameweekProvider
import providers.gameweek.GameweekSchema.gameweekStruct
import providers.understat.players.UnderstatPlayersSchema.understatPlayersStruct
import providers.understat.teams.UnderstatTeamsSchema.understatTeamsStruct
import providers.understat.players.UnderstatPlayersProvider
import providers.understat.teams.UnderstatTeamsProvider
import providers.unifiedData.{UnifiedPlayerDataProvider, UnifiedTeamDataProvider}
import writers.FileWriter

object Main {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("FPL Data Calculator")
      .master("local[*]")
      .getOrCreate()

    val gameweekDf: DataFrame = spark.read
      .option("header", value = true)
      .schema(gameweekStruct)
      .csv("data/2019-23 seasons.csv")

    val understatPlayersDf: DataFrame = spark.read
      .option("header", value = true)
      .schema(understatPlayersStruct)
      .csv("data/Understat - 2019-23 seasons.csv")

    val understatTeamsDf: DataFrame = spark.read
      .option("header", value = true)
      .schema(understatTeamsStruct)
      .csv("data/Understat Teams - 2019-23 seasons.csv")

    val fixturesDf: DataFrame = spark.read
      .option("header", value = true)
      .schema(fixturesStruct)
      .csv("data/Fixtures - 2019-23 seasons.csv")

    val gameweekFilteredDf: DataFrame = new GameweekProvider(gameweekDf).getData
    val understatPlayersFilteredDf: DataFrame = new UnderstatPlayersProvider(understatPlayersDf).getData
    val understatTeamsFilteredDf: DataFrame = new UnderstatTeamsProvider(understatTeamsDf).getData
    val fixturesFilteredDf: DataFrame = new FixturesProvider(fixturesDf).getData

    val unifiedPlayerDf: DataFrame = new UnifiedPlayerDataProvider(gameweekFilteredDf, understatPlayersFilteredDf).getData
    val unifiedTeamsDf: DataFrame = new UnifiedTeamDataProvider(fixturesFilteredDf, understatTeamsFilteredDf).getData

    val fileWriter: FileWriter = new FileWriter("data/temp", "data", "csv")
    fileWriter.writeToFile(unifiedTeamsDf)

    spark.stop()
  }
}
