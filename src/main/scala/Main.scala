import gameweek.GameweekSchema.gameweekStruct
import understat.UnderstatSchema.understatStruct
import gameweek.GameweekProvider
import org.apache.spark.sql.{DataFrame, SparkSession}
import understat.{UnderstatProvider, UnderstatTeamsProvider}
import understat.UnderstatTeamSchema.understatTeamStruct
import unifiedData.UnifiedDataProvider
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

    val understatDf: DataFrame = spark.read
      .option("header", value = true)
      .schema(understatStruct)
      .csv("data/Understat - 2019-23 seasons.csv")

    val understatTeamsDf: DataFrame = spark.read
      .option("header", value = true)
      .schema(understatTeamStruct)
      .csv("data/Understat Teams - 2019-23 seasons.csv")

    val gameweekFilteredDf: DataFrame = new GameweekProvider(gameweekDf).getData
    val understatFilteredDf: DataFrame = new UnderstatProvider(understatDf).getData
    val understatTeamsFilteredDf: DataFrame = new UnderstatTeamsProvider(understatTeamsDf).getData

    val unifiedDf: DataFrame = new UnifiedDataProvider(gameweekFilteredDf, understatFilteredDf).getData

    val fileWriter: FileWriter = new FileWriter("data/temp", "data", "csv")
    fileWriter.writeToFile(unifiedDf)

    spark.stop()
  }
}
