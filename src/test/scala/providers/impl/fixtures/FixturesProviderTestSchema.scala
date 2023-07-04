package providers.impl.fixtures

import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

object FixturesProviderTestSchema {

  val fixturesProviderTestStruct = new StructType(
    Array[StructField](
      StructField("team_a_difficulty", DataTypes.IntegerType, true, Metadata.empty),
      StructField("team_a_score", DataTypes.IntegerType, true, Metadata.empty),
      StructField("away_team", DataTypes.StringType, true, Metadata.empty),
      StructField("team_h_difficulty", DataTypes.IntegerType, true, Metadata.empty),
      StructField("home_team", DataTypes.StringType, true, Metadata.empty),
      StructField("team_h_score", DataTypes.IntegerType, true, Metadata.empty),
      StructField("date", DataTypes.StringType, true, Metadata.empty)
    )
  )
}
