package understat

import constants.UnderstatTeamColumns
import org.apache.spark.sql.DataFrame
import util.AverageCalculator.calculateRollingAvg

/* Temporary - will decide how to implement properly later */
object UnderstatTeamsHelper {

  def dropColumns(df: DataFrame): DataFrame = {
    df.drop(UnderstatTeamColumns.WINS,
      UnderstatTeamColumns.DEEP,
      UnderstatTeamColumns.PPDA_ALLOWED,
      UnderstatTeamColumns.MISSED,
      UnderstatTeamColumns.PPDA,
      UnderstatTeamColumns.PTS,
      UnderstatTeamColumns.XPTS,
      UnderstatTeamColumns.RESULT,
      UnderstatTeamColumns.LOSES,
      UnderstatTeamColumns.DRAWS
    )
  }

  def applyRollingAvg(df: DataFrame, numOfRows: Long): DataFrame = {
    val xGAvgDf: DataFrame = calculateRollingAvg(df, UnderstatTeamColumns.TEAM, UnderstatTeamColumns.X_G, numOfRows)
    val npxGDAvgDf: DataFrame = calculateRollingAvg(xGAvgDf, UnderstatTeamColumns.TEAM, UnderstatTeamColumns.NPX_G_D, numOfRows)
    val npxGAAvgDf: DataFrame = calculateRollingAvg(npxGDAvgDf, UnderstatTeamColumns.TEAM, UnderstatTeamColumns.NPX_G_A, numOfRows)
    val xGAAvgDf: DataFrame = calculateRollingAvg(npxGAAvgDf, UnderstatTeamColumns.TEAM, UnderstatTeamColumns.X_G_A, numOfRows)
    xGAAvgDf
  }
}
