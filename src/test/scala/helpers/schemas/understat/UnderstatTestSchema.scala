package helpers.schemas.understat

import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

object UnderstatTestSchema {

  val understatTestStruct = new StructType(
    Array[StructField](
      StructField("date", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("npxG", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("keyPasses", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("npg", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("xA", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("xG", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("name", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("shots", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("xGBuildup", DataTypes.DoubleType, nullable = true, Metadata.empty)
    )
  )
}
