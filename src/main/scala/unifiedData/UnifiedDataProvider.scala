package unifiedData

import constants.{CommonColumns, GameweekColumns}
import org.apache.spark.sql.DataFrame
import unifiedData.UnifiedDataHelper.{applyRollingAvg, dropColumnsAfterAvg, dropNullAvgs}
import util.AverageCalculator.calculateAvgAgainstOpponent

class UnifiedDataProvider(gameweekFilteredDf: DataFrame, understatFilteredDf: DataFrame) {

  def getData: DataFrame = {
    val joinedDf = joinData(Seq(CommonColumns.NAME, CommonColumns.DATE))
    val rollingAvgDf = applyRollingAvg(joinedDf, 5)
    val filteredColumnsDf = dropColumnsAfterAvg(rollingAvgDf).orderBy(CommonColumns.DATE)
    val avgAgainstTeamsDf = calculateAvgAgainstOpponent(filteredColumnsDf, GameweekColumns.TOTAL_POINTS)
    dropNullAvgs(avgAgainstTeamsDf)
  }

  private def joinData(columns: Seq[String]): DataFrame = {
    this.gameweekFilteredDf.join(this.understatFilteredDf, columns, "left_outer")
      .na.fill(0)
  }
}
