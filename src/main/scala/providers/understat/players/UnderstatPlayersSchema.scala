package providers.understat.players

import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

object UnderstatPlayersSchema {

  val understatPlayersStruct = new StructType(
    Array[StructField](
      StructField("date", DataTypes.DateType, true, Metadata.empty),
      StructField("npxG", DataTypes.DoubleType, true, Metadata.empty),
      StructField("key_passes", DataTypes.IntegerType, true, Metadata.empty),
      StructField("npg", DataTypes.IntegerType, true, Metadata.empty),
      StructField("xGChain", DataTypes.DoubleType, true, Metadata.empty),
      StructField("xA", DataTypes.DoubleType, true, Metadata.empty),
      StructField("h_goals", DataTypes.IntegerType, true, Metadata.empty),
      StructField("a_team", DataTypes.StringType, true, Metadata.empty),
      StructField("xG", DataTypes.DoubleType, true, Metadata.empty),
      StructField("roster_id", DataTypes.LongType, true, Metadata.empty),
      StructField("assists", DataTypes.IntegerType, true, Metadata.empty),
      StructField("name", DataTypes.StringType, true, Metadata.empty),
      StructField("season", DataTypes.IntegerType, true, Metadata.empty),
      StructField("a_goals", DataTypes.IntegerType, true, Metadata.empty),
      StructField("time", DataTypes.IntegerType, true, Metadata.empty),
      StructField("position", DataTypes.StringType, true, Metadata.empty),
      StructField("id", DataTypes.IntegerType, true, Metadata.empty),
      StructField("shots", DataTypes.IntegerType, true, Metadata.empty),
      StructField("h_team", DataTypes.StringType, true, Metadata.empty),
      StructField("xGBuildup", DataTypes.DoubleType, true, Metadata.empty),
      StructField("goals", DataTypes.IntegerType, true, Metadata.empty)
    )
  )
}
