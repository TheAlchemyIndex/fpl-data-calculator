import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import providers.impl.fixtures.FixturesProvider
import providers.util.schemas.FixturesSchema.fixturesStruct
import providers.util.schemas.GameweekSchema.gameweekStruct
import providers.impl.gameweek.GameweekProvider
import providers.impl.understat.players.UnderstatPlayersProvider
import providers.util.schemas.UnderstatPlayersSchema.understatPlayersStruct
import providers.impl.understat.teams.UnderstatTeamsProvider
import providers.util.schemas.UnderstatTeamsSchema.understatTeamsStruct
import providers.impl.unifiedData.players.UnifiedPlayersDataProvider
import providers.impl.unifiedData.teams.UnifiedTeamsDataProvider
import util.constants.FileNames
import writers.FileWriter

object Main {

  val LOGGER: Logger = Logger(LoggerFactory.getLogger(getClass.getName))

  def main(args: Array[String]): Unit = {
    val conf: Config = ConfigFactory.load("fpl_understat_processor.conf")
    val baseFilePath = conf.getString("conf.baseFilePath")
    val writeFilePath = conf.getString("conf.writeFilePath")
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
    LOGGER.info(s"Gameweek data loaded.")

    val understatPlayersFilteredDf: DataFrame = new UnderstatPlayersProvider(understatPlayersDf).getData
    LOGGER.info(s"Understat players data loaded.")

    val understatTeamsFilteredDf: DataFrame = new UnderstatTeamsProvider(understatTeamsDf).getData
    LOGGER.info(s"Understat teams data loaded.")

    val fixturesFilteredDf: DataFrame = new FixturesProvider(fixturesDf).getData
    LOGGER.info(s"Fixtures data loaded.")

    val unifiedPlayerDf: DataFrame = new UnifiedPlayersDataProvider(gameweekFilteredDf, understatPlayersFilteredDf).getData
    LOGGER.info(s"Unified players data created.")

    val unifiedTeamsDf: DataFrame = new UnifiedTeamsDataProvider(fixturesFilteredDf, understatTeamsFilteredDf).getData
    LOGGER.info(s"Unified teams data created.")

    val fileWriter: FileWriter = new FileWriter("csv", writeFilePath)
    fileWriter.writeToFile(unifiedPlayerDf, FileNames.UNIFIED_PLAYERS_CALC_FILENAME)
    fileWriter.writeToFile(unifiedTeamsDf, FileNames.UNIFIED_TEAMS_CALC_FILENAME)

    spark.stop()
  }
}
