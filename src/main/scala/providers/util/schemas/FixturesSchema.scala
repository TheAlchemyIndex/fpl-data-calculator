package providers.util.schemas

import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

object FixturesSchema {

  val fixturesStruct = new StructType(
    Array[StructField](
      StructField("code", DataTypes.LongType, true, Metadata.empty),
      StructField("provisional_start_time", DataTypes.BooleanType, true, Metadata.empty),
      StructField("minutes", DataTypes.IntegerType, true, Metadata.empty),
      StructField("kickoff_time", DataTypes.TimestampType, true, Metadata.empty),
      StructField("finished", DataTypes.BooleanType, true, Metadata.empty),
      StructField("started", DataTypes.BooleanType, true, Metadata.empty),
      StructField("team_a_difficulty", DataTypes.IntegerType, true, Metadata.empty),
      StructField("finished_provisional", DataTypes.BooleanType, true, Metadata.empty),
      StructField("team_a_score", DataTypes.IntegerType, true, Metadata.empty),
      StructField("away_team", DataTypes.StringType, true, Metadata.empty),
      StructField("stats", DataTypes.StringType, true, Metadata.empty),
      StructField("team_h_difficulty", DataTypes.IntegerType, true, Metadata.empty),
      StructField("id", DataTypes.IntegerType, true, Metadata.empty),
      StructField("event", DataTypes.IntegerType, true, Metadata.empty),
      StructField("home_team", DataTypes.StringType, true, Metadata.empty),
      StructField("team_h_score", DataTypes.IntegerType, true, Metadata.empty)
    )
  )
}
