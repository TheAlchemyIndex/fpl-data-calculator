package providers.impl

import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

object GameweekTestSchema {

  val gameweekTestStruct: StructType = new StructType(
    Array[StructField](
      StructField("transfers_balance", DataTypes.LongType, nullable = true, Metadata.empty),
      StructField("opponent_team", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("bonus", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("own_goals", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("clean_sheets", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("goals_conceded", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("total_points", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("kickoff_time", DataTypes.TimestampType, nullable = true, Metadata.empty),
      StructField("red_cards", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("team_a_score", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("influence", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("saves", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("assists", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("transfers_in", DataTypes.LongType, nullable = true, Metadata.empty),
      StructField("xP", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("season", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("creativity", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("value", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("selected", DataTypes.LongType, nullable = true, Metadata.empty),
      StructField("element", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("goals_scored", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("bps", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("was_home", DataTypes.BooleanType, nullable = true, Metadata.empty),
      StructField("minutes", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("penalties_missed", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("yellow_cards", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("team", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("fixture", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("transfers_out", DataTypes.LongType, nullable = true, Metadata.empty),
      StructField("round", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("name", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("threat", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("position", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("ict_index", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("penalties_saved", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("team_h_score", DataTypes.IntegerType, nullable = true, Metadata.empty)
    )
  )
}
