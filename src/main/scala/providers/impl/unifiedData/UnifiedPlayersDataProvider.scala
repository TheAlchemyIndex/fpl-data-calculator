package providers.impl.unifiedData

import org.apache.spark.sql.DataFrame
import providers.Provider
import providers.util.AverageCalculator.calculateRollingAvg
import util.constants.{CalculatedColumns, CommonColumns, GameweekColumns, UnderstatPlayersColumns}

class UnifiedPlayersDataProvider(gameweekDf: DataFrame, understatDf: DataFrame) extends Provider {

  def getData: DataFrame = {
    val joinedDf: DataFrame = this.gameweekDf
      .join(this.understatDf, Seq(CommonColumns.NAME, CommonColumns.DATE), "left_outer")
      .na.fill(0)

    val rollingAvgDf: DataFrame = applyRollingAvg(joinedDf, 5)

    dropColumns(rollingAvgDf)
      .orderBy(CommonColumns.DATE)
      .na.drop(Seq(CalculatedColumns.BONUS_AVG))
  }

  private def applyRollingAvg(df: DataFrame, numOfRows: Int): DataFrame = {
    val columnsToAvg = List(
      GameweekColumns.BONUS,
      GameweekColumns.CLEAN_SHEETS,
      GameweekColumns.GOALS_CONCEDED,
      GameweekColumns.TOTAL_POINTS,
      GameweekColumns.INFLUENCE,
      GameweekColumns.ASSISTS,
      GameweekColumns.CREATIVITY,
      GameweekColumns.VALUE,
      GameweekColumns.GOALS_SCORED,
      GameweekColumns.MINUTES,
      GameweekColumns.YELLOW_CARDS,
      GameweekColumns.THREAT,
      GameweekColumns.ICT_INDEX,
      UnderstatPlayersColumns.NPX_G,
      UnderstatPlayersColumns.KEY_PASSES,
      UnderstatPlayersColumns.NPG,
      UnderstatPlayersColumns.X_A,
      UnderstatPlayersColumns.X_G,
      UnderstatPlayersColumns.SHOTS,
      UnderstatPlayersColumns.X_G_BUILDUP
    )

    columnsToAvg.foldLeft(df) { (currentDf, column) =>
      calculateRollingAvg(currentDf, CommonColumns.NAME, column, numOfRows)
    }
  }

  private def dropColumns(df: DataFrame): DataFrame = {
    val columnsToDrop = List(
      GameweekColumns.BONUS,
      GameweekColumns.CLEAN_SHEETS,
      GameweekColumns.GOALS_CONCEDED,
      GameweekColumns.TEAM_A_SCORE,
      GameweekColumns.INFLUENCE,
      GameweekColumns.TRANSFERS_IN,
      GameweekColumns.SAVES,
      GameweekColumns.ASSISTS,
      GameweekColumns.CREATIVITY,
      GameweekColumns.VALUE,
      GameweekColumns.SELECTED,
      GameweekColumns.GOALS_SCORED,
      GameweekColumns.YELLOW_CARDS,
      GameweekColumns.TRANSFERS_OUT,
      GameweekColumns.THREAT,
      GameweekColumns.ICT_INDEX,
      GameweekColumns.PENALTIES_SAVED,
      GameweekColumns.TEAM_H_SCORE,
      UnderstatPlayersColumns.NPX_G,
      UnderstatPlayersColumns.KEY_PASSES,
      UnderstatPlayersColumns.NPG,
      UnderstatPlayersColumns.X_A,
      UnderstatPlayersColumns.X_G,
      UnderstatPlayersColumns.SHOTS,
      UnderstatPlayersColumns.X_G_BUILDUP
    )

    df.drop(columnsToDrop: _*)
  }
}
