package providers.impl.gameweek

import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

object GameweekProviderTestSchema {

  val gameweekProviderTestStruct: StructType = new StructType(
    Array[StructField](
      StructField("opponent_team", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("bonus", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("clean_sheets", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("goals_conceded", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("total_points", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("influence", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("saves", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("assists", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("transfers_in", DataTypes.LongType, nullable = true, Metadata.empty),
      StructField("xP", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("season", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("creativity", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("value", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("selected", DataTypes.LongType, nullable = true, Metadata.empty),
      StructField("goals_scored", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("minutes", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("yellow_cards", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("team", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("transfers_out", DataTypes.LongType, nullable = true, Metadata.empty),
      StructField("round", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("name", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("threat", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("position", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("ict_index", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("penalties_saved", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("home_fixture", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("date", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("month", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("year", DataTypes.IntegerType, nullable = true, Metadata.empty)
    )
  )
}
