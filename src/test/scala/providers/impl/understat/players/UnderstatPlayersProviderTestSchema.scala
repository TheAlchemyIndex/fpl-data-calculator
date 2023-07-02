package providers.impl.understat.players

import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

object UnderstatPlayersProviderTestSchema {

  val understatPlayersProviderTestStruct = new StructType(
    Array[StructField](
      StructField("date", DataTypes.StringType, true, Metadata.empty),
      StructField("x_g", DataTypes.DoubleType, true, Metadata.empty),
      StructField("key_passes", DataTypes.IntegerType, true, Metadata.empty),
      StructField("npg", DataTypes.IntegerType, true, Metadata.empty),
      StructField("name", DataTypes.StringType, true, Metadata.empty),
      StructField("x_a", DataTypes.DoubleType, true, Metadata.empty),
      StructField("x_g_buildup", DataTypes.DoubleType, true, Metadata.empty),
      StructField("shots", DataTypes.IntegerType, true, Metadata.empty),
      StructField("npx_g", DataTypes.DoubleType, true, Metadata.empty)
    )
  )
}
