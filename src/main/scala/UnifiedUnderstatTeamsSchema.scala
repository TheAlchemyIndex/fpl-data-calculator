import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

object UnifiedUnderstatTeamsSchema {

  val unifiedUnderstatTeamsStruct = new StructType(
    Array[StructField](
      StructField("date", DataTypes.DateType, true, Metadata.empty),
      StructField("homeTeam", DataTypes.StringType, true, Metadata.empty),
      StructField("homeNpxG", DataTypes.DoubleType, true, Metadata.empty),
      StructField("homeXG", DataTypes.DoubleType, true, Metadata.empty),
      StructField("homeNpxGD", DataTypes.DoubleType, true, Metadata.empty),
      StructField("homeNpxGA", DataTypes.DoubleType, true, Metadata.empty),
      StructField("homeXGA", DataTypes.DoubleType, true, Metadata.empty),
      StructField("homeScored", DataTypes.IntegerType, true, Metadata.empty),
      StructField("homeDifficulty", DataTypes.IntegerType, true, Metadata.empty),
      StructField("awayTeam", DataTypes.StringType, true, Metadata.empty),
      StructField("awayNpxG", DataTypes.DoubleType, true, Metadata.empty),
      StructField("awayXG", DataTypes.DoubleType, true, Metadata.empty),
      StructField("awayNpxGD", DataTypes.DoubleType, true, Metadata.empty),
      StructField("awayNpxGA", DataTypes.DoubleType, true, Metadata.empty),
      StructField("awayXGA", DataTypes.DoubleType, true, Metadata.empty),
      StructField("awayScored", DataTypes.IntegerType, true, Metadata.empty),
      StructField("awayDifficulty", DataTypes.IntegerType, true, Metadata.empty)
    )
  )
}
