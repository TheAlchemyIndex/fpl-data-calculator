package helpers.schemas

import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

object JoinedDataFilteredSchema {

  val joinedDataFilteredStruct: StructType = new StructType(
    Array[StructField](
      StructField("name", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("unformattedDate", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("opponentTeam", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("totalPoints", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("xP", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("team", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("round", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("position", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("homeFixture", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("month", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("year", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("bonusAvg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("cleanSheetsAvg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("goalsConcededAvg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("totalPointsAvg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("influenceAvg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("assistsAvg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("creativityAvg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("valueAvg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("goalsScoredAvg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("minutesAvg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("yellowCardsAvg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("threatAvg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("ictIndexAvg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("npxGAvg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("keyPassesAvg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("npgAvg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("xAAvg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("xGAvg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("shotsAvg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("xGBuildupAvg", DataTypes.DoubleType, nullable = true, Metadata.empty)
    )
  )
}
