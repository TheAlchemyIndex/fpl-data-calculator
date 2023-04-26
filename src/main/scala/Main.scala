import DateFormatter.extractDateElements
import GameweekHelper.{booleanColumnToBinary, dropColumnsAfterAvg, dropUnnecessaryColumns}
import GameweekSchema.gameweekStruct
import RollingAverage.applyRollingAvg
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("FPL Data Calculator")
      .master("local[*]")
      .getOrCreate()

    val gameweeksDf = spark.read
      .option("header", value = true)
      .schema(gameweekStruct)
      .csv("data/2019-23 seasons.csv")

    val gameweeksBooleanConvertedDf = booleanColumnToBinary(gameweeksDf, "home_fixture", "was_home")
    val gameweeksDateDf = extractDateElements(gameweeksBooleanConvertedDf)
    val gameweeksFilteredDf = dropUnnecessaryColumns(gameweeksDateDf)
    val rollingAvgDf = applyRollingAvg(gameweeksFilteredDf, 5)
    val rollingAvgFilteredDf = dropColumnsAfterAvg(rollingAvgDf)

    rollingAvgFilteredDf.printSchema()
    rollingAvgFilteredDf.show()

    spark.stop()
  }
}