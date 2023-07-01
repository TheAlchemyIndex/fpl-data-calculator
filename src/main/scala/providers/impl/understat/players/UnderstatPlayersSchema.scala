package providers.impl.understat.players

import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

object UnderstatPlayersSchema {

  val understatPlayersStruct = new StructType(
    Array[StructField](
      StructField("date", DataTypes.DateType, true, Metadata.empty),
      StructField("npx_g", DataTypes.DoubleType, true, Metadata.empty),
      StructField("key_passes", DataTypes.IntegerType, true, Metadata.empty),
      StructField("npg", DataTypes.IntegerType, true, Metadata.empty),
      StructField("x_g_chain", DataTypes.DoubleType, true, Metadata.empty),
      StructField("x_a", DataTypes.DoubleType, true, Metadata.empty),
      StructField("h_goals", DataTypes.IntegerType, true, Metadata.empty),
      StructField("a_team", DataTypes.StringType, true, Metadata.empty),
      StructField("x_g", DataTypes.DoubleType, true, Metadata.empty),
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
      StructField("x_g_buildup", DataTypes.DoubleType, true, Metadata.empty),
      StructField("goals", DataTypes.IntegerType, true, Metadata.empty)
    )
  )
}
