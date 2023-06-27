import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import providers.fixtures.FixturesProvider
import providers.fixtures.FixturesSchema.fixturesStruct
import providers.gameweek.GameweekProvider
import providers.gameweek.GameweekSchema.gameweekStruct
import providers.understat.players.UnderstatPlayersSchema.understatPlayersStruct
import providers.understat.teams.UnderstatTeamsSchema.understatTeamsStruct
import providers.understat.players.UnderstatPlayersProvider
import providers.understat.teams.UnderstatTeamsProvider
import providers.unifiedData.{UnifiedPlayersDataProvider, UnifiedTeamsDataProvider}
import writers.FileWriter

object Main {

  def main(args: Array[String]): Unit = {
    val conf: Config = ConfigFactory.load("fpl_understat_processor.conf")
    val baseFilePath = conf.getString("conf.baseFilePath")
    val gameweekFilePath = baseFilePath + conf.getString("conf.gameweekFileName")
    val understatPlayersFilePath = baseFilePath + conf.getString("conf.understatPlayersFileName")
    val understatTeamsFilePath = baseFilePath + conf.getString("conf.understatTeamsFileName")
    val fixturesFilePath = baseFilePath + conf.getString("conf.fixturesFileName")

    val spark = SparkSession.builder()
      .appName("FPL and Understat Data Processor")
      .master("local[*]")
      .getOrCreate()

    val gameweekDf: DataFrame = spark.read
      .option("header", value = true)
      .schema(gameweekStruct)
      .csv(gameweekFilePath)

    val understatPlayersDf: DataFrame = spark.read
      .option("header", value = true)
      .schema(understatPlayersStruct)
      .csv(understatPlayersFilePath)

    val understatTeamsDf: DataFrame = spark.read
      .option("header", value = true)
      .schema(understatTeamsStruct)
      .csv(understatTeamsFilePath)

    val fixturesDf: DataFrame = spark.read
      .option("header", value = true)
      .schema(fixturesStruct)
      .csv(fixturesFilePath)

    val gameweekFilteredDf: DataFrame = new GameweekProvider(gameweekDf).getData
    val understatPlayersFilteredDf: DataFrame = new UnderstatPlayersProvider(understatPlayersDf).getData
    val understatTeamsFilteredDf: DataFrame = new UnderstatTeamsProvider(understatTeamsDf).getData
    val fixturesFilteredDf: DataFrame = new FixturesProvider(fixturesDf).getData

    val unifiedPlayerDf: DataFrame = new UnifiedPlayersDataProvider(gameweekFilteredDf, understatPlayersFilteredDf).getData
    val unifiedTeamsDf: DataFrame = new UnifiedTeamsDataProvider(fixturesFilteredDf, understatTeamsFilteredDf).getData

    val fileWriter: FileWriter = new FileWriter("csv")
    fileWriter.writeToFile(unifiedPlayerDf, "data", "understat_players_calc_data.csv")
    fileWriter.writeToFile(unifiedTeamsDf, "data", "understat_teams_calc_data.csv")

    spark.stop()
  }
}
