package providers.impl.understat.teams

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, to_date, udf, when}
import providers.Provider
import util.constants.{CommonColumns, TemporaryRenamedColumns, UnderstatTeamsColumns}

class UnderstatTeamsProvider(understatTeamsDf: DataFrame) extends Provider {

  def getData: DataFrame = {
    val dateFormattedDf: DataFrame = understatTeamsDf
      .withColumn(CommonColumns.DATE, to_date(col(UnderstatTeamsColumns.DATE), "yyyy-MM-dd"))
      .withColumn(UnderstatTeamsColumns.TEAM_2_TYPE, when(col(UnderstatTeamsColumns.H_A) === "h", "a")
        .when(col(UnderstatTeamsColumns.H_A) === "a", "h"))
      .withColumnRenamed(UnderstatTeamsColumns.H_A, UnderstatTeamsColumns.TEAM_1_TYPE)

    val ppdaFormattedDf: DataFrame = extractPpda(dateFormattedDf)
    dropColumns(ppdaFormattedDf)
  }

  private def extractPpda(df: DataFrame): DataFrame = {
    df
      .withColumn(TemporaryRenamedColumns.PPDA_ALLOWED_ATT_DEF, ppdaPatternMatcher(col(UnderstatTeamsColumns.PPDA_ALLOWED)))
      .withColumn(TemporaryRenamedColumns.PPDA_ATT_DEF, ppdaPatternMatcher(col(UnderstatTeamsColumns.PPDA)))
      .withColumn(UnderstatTeamsColumns.PPDA_ALLOWED_ATT, col(TemporaryRenamedColumns.PPDA_ALLOWED_ATT_DEF).getField("_1"))
      .withColumn(UnderstatTeamsColumns.PPDA_ALLOWED_DEF, col(TemporaryRenamedColumns.PPDA_ALLOWED_ATT_DEF).getField("_2"))
      .withColumn(UnderstatTeamsColumns.PPDA_ATT, col(TemporaryRenamedColumns.PPDA_ATT_DEF).getField("_1"))
      .withColumn(UnderstatTeamsColumns.PPDA_DEF, col(TemporaryRenamedColumns.PPDA_ATT_DEF).getField("_2"))
      .drop(TemporaryRenamedColumns.PPDA_ALLOWED_ATT_DEF)
      .drop(TemporaryRenamedColumns.PPDA_ATT_DEF)
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
