import gameweek.GameweekSchema.gameweekStruct
import understat.UnderstatSchema.understatStruct
import gameweek.GameweekProvider
import org.apache.spark.sql.SparkSession
import understat.UnderstatProvider
import unifiedData.UnifiedDataProvider

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("FPL Data Calculator")
      .master("local[*]")
      .getOrCreate()

    val gameweekDf = spark.read
      .option("header", value = true)
      .schema(gameweekStruct)
      .csv("data/2019-23 seasons.csv")

    val understatDf = spark.read
      .option("header", value = true)
      .schema(understatStruct)
      .csv("data/Understat - 2019-23 seasons.csv")

    val gameweekFilteredDf = new GameweekProvider(gameweekDf).getData
    val understatFilteredDf = new UnderstatProvider(understatDf).getData
    val unifiedDf = new UnifiedDataProvider(gameweekFilteredDf, understatFilteredDf).getData

//    unifiedDf.write
//      .option("header", "true")
//      .format("csv")
//      .save("data/out")

    spark.stop()
  }
}
