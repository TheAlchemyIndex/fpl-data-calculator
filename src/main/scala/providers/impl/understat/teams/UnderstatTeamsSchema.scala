package providers.impl.understat.teams

import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

object UnderstatTeamsSchema {

  val understatTeamsStruct = new StructType(
    Array[StructField](
      StructField("date", DataTypes.TimestampType, true, Metadata.empty),
      StructField("wins", DataTypes.IntegerType, true, Metadata.empty),
      StructField("npx_g", DataTypes.DoubleType, true, Metadata.empty),
      StructField("deep", DataTypes.IntegerType, true, Metadata.empty),
      /* Will replace with StructType later */
      StructField("ppda_allowed", DataTypes.StringType, true, Metadata.empty),
      StructField("missed", DataTypes.IntegerType, true, Metadata.empty),
      /* Will replace with StructType later */
      StructField("ppda", DataTypes.StringType, true, Metadata.empty),
      StructField("h_a", DataTypes.StringType, true, Metadata.empty),
      StructField("team", DataTypes.StringType, true, Metadata.empty),
      StructField("pts", DataTypes.IntegerType, true, Metadata.empty),
      StructField("x_g", DataTypes.DoubleType, true, Metadata.empty),
      StructField("xpts", DataTypes.DoubleType, true, Metadata.empty),
      StructField("result", DataTypes.StringType, true, Metadata.empty),
      StructField("npx_g_d", DataTypes.DoubleType, true, Metadata.empty),
      StructField("npx_g_a", DataTypes.DoubleType, true, Metadata.empty),
      StructField("x_g_a", DataTypes.DoubleType, true, Metadata.empty),
      StructField("deep_allowed", DataTypes.IntegerType, true, Metadata.empty),
      StructField("scored", DataTypes.IntegerType, true, Metadata.empty),
      StructField("loses", DataTypes.IntegerType, true, Metadata.empty),
      StructField("draws", DataTypes.IntegerType, true, Metadata.empty),
      StructField("season", DataTypes.StringType, true, Metadata.empty)
    )
  )
}
