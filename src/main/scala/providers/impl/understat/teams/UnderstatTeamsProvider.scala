package providers.impl.understat.teams

import constants.{CommonColumns, UnderstatTeamsColumns}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, to_date, udf, when}
import providers.Provider

class UnderstatTeamsProvider(understatTeamsDf: DataFrame) extends Provider {

  def getData: DataFrame = {
    val dateFormattedDf: DataFrame = understatTeamsDf
      .withColumn(CommonColumns.DATE, to_date(col(UnderstatTeamsColumns.DATE), "yyyy-MM-dd"))
      .withColumn("team2Type", when(col(UnderstatTeamsColumns.H_A) === "h", "a").otherwise("h"))
      .withColumnRenamed(UnderstatTeamsColumns.H_A, "team1Type")

    val ppdaFormattedDf: DataFrame = extractPpda(dateFormattedDf)
    dropColumns(ppdaFormattedDf)
  }

  private def extractPpda(df: DataFrame): DataFrame = {
    df
      .withColumn("ppda_allowed_att_def", ppdaPatternMatcher(col(UnderstatTeamsColumns.PPDA_ALLOWED)))
      .withColumn("ppda_att_def", ppdaPatternMatcher(col(UnderstatTeamsColumns.PPDA)))
      .withColumn("ppdaAllowedAtt", col("ppda_allowed_att_def").getField("_1"))
      .withColumn("ppdaAllowedDef", col("ppda_allowed_att_def").getField("_2"))
      .withColumn("ppdaAtt", col("ppda_att_def").getField("_1"))
      .withColumn("ppdaDef", col("ppda_att_def").getField("_2"))
      .drop("ppda_allowed_att_def")
      .drop("ppda_att_def")
  }

  private def ppdaPatternMatcher(data: Column): Column = {
    val regex = """\{att:(\d+),def:(\d+)\}""".r
    val parseUDF = udf((data: String) => {
      data match {
        case regex(att, defValue) => (att.toInt, defValue.toInt)
        case _ => (0, 0)
      }
    })
    parseUDF(data)
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
