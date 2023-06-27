package providers.understat.teams

import constants.{CommonColumns, UnderstatTeamsColumns}
import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions.{col, udf, when}
import providers.Provider
import util.DataFrameHelper.renameColumnsToCamelCase
import util.DateFormatter.timestampToDate

class UnderstatTeamsProvider(understatTeamsDf: DataFrame) extends Provider {

  def getData: DataFrame = {
    val camelCaseDf: DataFrame = renameColumnsToCamelCase(understatTeamsDf)
    val timestampToDateDf: DataFrame = timestampToDate(camelCaseDf, CommonColumns.DATE, UnderstatTeamsColumns.DATE)
      .withColumn("team2Type", when(col(UnderstatTeamsColumns.H_A) === "h", "a").otherwise("h"))
      .withColumnRenamed(UnderstatTeamsColumns.H_A, "team1Type")

    val parseData = udf((data: String) => {
      val regex = """\{att:(\d+),def:(\d+)\}""".r
      data match {
        case regex(att, defValue) => (att.toInt, defValue.toInt)
        case _ => (0, 0) // Default values if the format doesn't match
      }
    })

    val explodedDF = timestampToDateDf
      .withColumn("ppda_allowed_att_def", parseData(col(UnderstatTeamsColumns.PPDA_ALLOWED)))
      .withColumn("ppda_att_def", parseData(col(UnderstatTeamsColumns.PPDA)))
      .withColumn("ppdaAllowedAtt", col("ppda_allowed_att_def").getField("_1"))
      .withColumn("ppdaAllowedDef", col("ppda_allowed_att_def").getField("_2"))
      .withColumn("ppdaAtt", col("ppda_att_def").getField("_1"))
      .withColumn("ppdaDef", col("ppda_att_def").getField("_2"))
      .drop("ppda_allowed_att_def")
      .drop("ppda_att_def")

    dropColumns(explodedDF)
  }

  private def dropColumns(df: DataFrame): DataFrame = {
    df.drop(UnderstatTeamsColumns.WINS,
      UnderstatTeamsColumns.DEEP,
      UnderstatTeamsColumns.PPDA_ALLOWED,
      UnderstatTeamsColumns.MISSED,
      UnderstatTeamsColumns.PPDA,
      UnderstatTeamsColumns.PTS,
      UnderstatTeamsColumns.XPTS,
      UnderstatTeamsColumns.RESULT,
      UnderstatTeamsColumns.DEEP_ALLOWED,
      UnderstatTeamsColumns.SCORED,
      UnderstatTeamsColumns.LOSES,
      UnderstatTeamsColumns.DRAWS
    )
  }
}
