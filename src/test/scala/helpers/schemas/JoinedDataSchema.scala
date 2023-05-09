package helpers.schemas

import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

object JoinedDataSchema {

  val joinedDataStruct: StructType = new StructType(
    Array[StructField](
      StructField("name", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("unformattedDate", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("opponentTeam", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("bonus", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("cleanSheets", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("goalsConceded", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("totalPoints", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("teamAScore", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("influence", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("saves", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("assists", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("transfersIn", DataTypes.LongType, nullable = true, Metadata.empty),
      StructField("xP", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("creativity", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("value", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("selected", DataTypes.LongType, nullable = true, Metadata.empty),
      StructField("goalsScored", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("minutes", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("yellowCards", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("team", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("transfersOut", DataTypes.LongType, nullable = true, Metadata.empty),
      StructField("round", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("position", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("threat", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("webName", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("ictIndex", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("penaltiesSaved", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("teamHScore", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("homeFixture", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("month", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("year", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("npxG", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("keyPasses", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("npg", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("xA", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("xG", DataTypes.DoubleType, nullable = true, Metadata.empty),
      StructField("shots", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("xGBuildup", DataTypes.DoubleType, nullable = true, Metadata.empty)
    )
  )
}
