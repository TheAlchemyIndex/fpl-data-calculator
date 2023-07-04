package providers.impl.unifiedData.players

import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

object UnifiedPlayersDataProviderTestSchema {

  val unifiedPlayersDataProviderTestStruct: StructType = new StructType(
    Array[StructField](
      StructField("name", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("date", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("opponent_team", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("total_points", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("xP", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("season", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("minutes", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("team", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("round", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("position", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("home_fixture", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("month", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("year", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("bonus_avg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("clean_sheets_avg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("goals_conceded_avg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("total_points_avg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("influence_avg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("assists_avg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("creativity_avg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("value_avg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("goals_scored_avg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("minutes_avg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("yellow_cards_avg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("threat_avg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("ict_index_avg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("npx_g_avg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("key_passes_avg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("npg_avg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("x_a_avg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("x_g_avg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("shots_avg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("x_g_buildup_avg", DataTypes.DoubleType, nullable = true, Metadata.empty)
    )
  )
}
