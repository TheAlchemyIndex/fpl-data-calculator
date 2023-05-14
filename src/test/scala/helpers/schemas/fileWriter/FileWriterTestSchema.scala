package helpers.schemas.fileWriter

import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

object FileWriterTestSchema {

  val fileWriterTestStruct: StructType = new StructType(
    Array[StructField](
      StructField("col1", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("col2", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("col3", DataTypes.IntegerType, nullable = true, Metadata.empty)
    )
  )
}
