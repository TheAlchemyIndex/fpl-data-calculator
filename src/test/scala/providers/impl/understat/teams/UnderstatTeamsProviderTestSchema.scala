package providers.impl.understat.teams

import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

object UnderstatTeamsProviderTestSchema {

  val understatTeamsProviderTestStruct = new StructType(
    Array[StructField](
      StructField("date", DataTypes.StringType, true, Metadata.empty),
      StructField("x_g", DataTypes.DoubleType, true, Metadata.empty),
      StructField("x_g_a", DataTypes.DoubleType, true, Metadata.empty),
      StructField("team_1_type", DataTypes.StringType, true, Metadata.empty),
      StructField("team", DataTypes.StringType, true, Metadata.empty),
      StructField("npx_g_d", DataTypes.DoubleType, true, Metadata.empty),
      StructField("npx_g_a", DataTypes.DoubleType, true, Metadata.empty),
      StructField("season", DataTypes.StringType, true, Metadata.empty),
      StructField("npx_g", DataTypes.DoubleType, true, Metadata.empty),
      StructField("team_2_type", DataTypes.StringType, true, Metadata.empty),
      StructField("ppda_allowed_att", DataTypes.IntegerType, true, Metadata.empty),
      StructField("ppda_allowed_def", DataTypes.IntegerType, true, Metadata.empty),
      StructField("ppda_att", DataTypes.IntegerType, true, Metadata.empty),
      StructField("ppda_def", DataTypes.IntegerType, true, Metadata.empty)
    )
  )
}
