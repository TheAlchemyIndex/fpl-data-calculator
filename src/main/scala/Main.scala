import GameweekHelper.{booleanColumnToBinary, dropExtraColumns}
import GameweekSchema.gameweekStruct
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
    val gameweeksFilteredDf = dropExtraColumns(gameweeksBooleanConvertedDf)

    gameweeksFilteredDf.printSchema()
    gameweeksFilteredDf.show()

    spark.stop()
  }
}