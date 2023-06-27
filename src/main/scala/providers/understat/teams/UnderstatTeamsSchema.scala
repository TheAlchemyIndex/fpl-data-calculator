package providers.understat.teams

import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

object UnderstatTeamsSchema {

  val ppdaStruct = new StructType(
    Array[StructField](
    StructField("att", DataTypes.IntegerType, true, Metadata.empty),
    StructField("def", DataTypes.IntegerType, true, Metadata.empty)
    )
  )

  val understatTeamsStruct = new StructType(
    Array[StructField](
      StructField("date", DataTypes.TimestampType, true, Metadata.empty),
      StructField("wins", DataTypes.IntegerType, true, Metadata.empty),
      StructField("npxG", DataTypes.DoubleType, true, Metadata.empty),
      StructField("deep", DataTypes.IntegerType, true, Metadata.empty),
      /* Will replace with StructType later */
      StructField("ppda_allowed", DataTypes.StringType, true, Metadata.empty),
      StructField("missed", DataTypes.IntegerType, true, Metadata.empty),
      /* Will replace with StructType later */
      StructField("ppda", DataTypes.StringType, true, Metadata.empty),
      StructField("h_a", DataTypes.StringType, true, Metadata.empty),
      StructField("team", DataTypes.StringType, true, Metadata.empty),
      StructField("pts", DataTypes.IntegerType, true, Metadata.empty),
      StructField("xG", DataTypes.DoubleType, true, Metadata.empty),
      StructField("xpts", DataTypes.DoubleType, true, Metadata.empty),
      StructField("result", DataTypes.StringType, true, Metadata.empty),
      StructField("npxGD", DataTypes.DoubleType, true, Metadata.empty),
      StructField("npxGA", DataTypes.DoubleType, true, Metadata.empty),
      StructField("xGA", DataTypes.DoubleType, true, Metadata.empty),
      StructField("deep_allowed", DataTypes.IntegerType, true, Metadata.empty),
      StructField("scored", DataTypes.IntegerType, true, Metadata.empty),
      StructField("loses", DataTypes.IntegerType, true, Metadata.empty),
      StructField("draws", DataTypes.IntegerType, true, Metadata.empty),
      StructField("season", DataTypes.StringType, true, Metadata.empty)
    )
  )
}
