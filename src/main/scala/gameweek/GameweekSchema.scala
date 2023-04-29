package gameweek

import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

object GameweekSchema {

  val gameweekStruct = new StructType(
    Array[StructField](
      StructField("transfers_balance", DataTypes.LongType, true, Metadata.empty),
      StructField("opponent_team", DataTypes.StringType, true, Metadata.empty),
      StructField("bonus", DataTypes.IntegerType, true, Metadata.empty),
      StructField("own_goals", DataTypes.IntegerType, true, Metadata.empty),
      StructField("clean_sheets", DataTypes.IntegerType, true, Metadata.empty),
      StructField("goals_conceded", DataTypes.IntegerType, true, Metadata.empty),
      StructField("total_points", DataTypes.IntegerType, true, Metadata.empty),
      StructField("kickoff_time", DataTypes.TimestampType, true, Metadata.empty),
      StructField("red_cards", DataTypes.IntegerType, true, Metadata.empty),
      StructField("team_a_score", DataTypes.IntegerType, true, Metadata.empty),
      StructField("influence", DataTypes.DoubleType, true, Metadata.empty),
      StructField("saves", DataTypes.IntegerType, true, Metadata.empty),
      StructField("assists", DataTypes.IntegerType, true, Metadata.empty),
      StructField("transfers_in", DataTypes.LongType, true, Metadata.empty),
      StructField("creativity", DataTypes.DoubleType, true, Metadata.empty),
      StructField("value", DataTypes.IntegerType, true, Metadata.empty),
      StructField("selected", DataTypes.LongType, true, Metadata.empty),
      StructField("element", DataTypes.IntegerType, true, Metadata.empty),
      StructField("goals_scored", DataTypes.IntegerType, true, Metadata.empty),
      StructField("bps", DataTypes.IntegerType, true, Metadata.empty),
      StructField("was_home", DataTypes.BooleanType, true, Metadata.empty),
      StructField("minutes", DataTypes.IntegerType, true, Metadata.empty),
      StructField("penalties_missed", DataTypes.IntegerType, true, Metadata.empty),
      StructField("yellow_cards", DataTypes.IntegerType, true, Metadata.empty),
      StructField("team", DataTypes.StringType, true, Metadata.empty),
      StructField("fixture", DataTypes.IntegerType, true, Metadata.empty),
      StructField("transfers_out", DataTypes.LongType, true, Metadata.empty),
      StructField("round", DataTypes.IntegerType, true, Metadata.empty),
      StructField("name", DataTypes.StringType, true, Metadata.empty),
      StructField("threat", DataTypes.DoubleType, true, Metadata.empty),
      StructField("position", DataTypes.StringType, true, Metadata.empty),
      StructField("ict_index", DataTypes.DoubleType, true, Metadata.empty),
      StructField("penalties_saved", DataTypes.IntegerType, true, Metadata.empty),
      StructField("team_h_score", DataTypes.IntegerType, true, Metadata.empty)
    )
  )
}
