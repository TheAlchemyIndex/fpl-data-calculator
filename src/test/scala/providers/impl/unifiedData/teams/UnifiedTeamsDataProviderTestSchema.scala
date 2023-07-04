package providers.impl.unifiedData.teams

import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

object UnifiedTeamsDataProviderTestSchema {

  val unifiedTeamsDataProviderTestStruct: StructType = new StructType(
    Array[StructField](
      StructField("date", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("team_2", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("team_1", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("team_2_difficulty", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("team_1_difficulty", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("team1_type", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("team2_type", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("ppda_allowed_att", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("ppda_allowed_def", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("ppda_att", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("ppda_def", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("season", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("both_scored", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("team_1_score_avg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("team_1_npx_g_avg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("team_1_x_g_avg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("team_1_npx_g_d_avg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("team_1_npx_g_a_avg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("team_1_x_g_a_avg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("team_1_clean_sheet_avg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("team_2_score_avg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("team_2_npx_g_avg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("team_2_x_g_avg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("team_2_npx_g_d_avg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("team_2_npx_g_a_avg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("team_2_x_g_a_avg", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("team_2_clean_sheet_avg", DataTypes.DoubleType, nullable = true, Metadata.empty)
    )
  )
}
