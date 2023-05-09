package unifiedData

import constants.{CalculatedColumns, CommonColumns, GameweekColumns, UnderstatColumns}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object UnifiedDataHelper {

  def dropColumnsAfterAvg(df: DataFrame): DataFrame = {
    df
      .drop(GameweekColumns.BONUS,
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
        GameweekColumns.MINUTES,
        GameweekColumns.YELLOW_CARDS,
        GameweekColumns.TRANSFERS_OUT,
        GameweekColumns.THREAT,
        GameweekColumns.ICT_INDEX,
        GameweekColumns.PENALTIES_SAVED,
        GameweekColumns.TEAM_H_SCORE,
        UnderstatColumns.NPX_G,
        UnderstatColumns.KEY_PASSES,
        UnderstatColumns.NPG,
        UnderstatColumns.X_A,
        UnderstatColumns.X_G,
        UnderstatColumns.SHOTS,
        UnderstatColumns.X_G_BUILDUP)
  }

  def dropColumnsAfterJoin(df: DataFrame): DataFrame = {
    df
      .drop(UnderstatColumns.NPX_G,
        UnderstatColumns.KEY_PASSES,
        UnderstatColumns.NPG,
        UnderstatColumns.X_A,
        UnderstatColumns.X_G,
        UnderstatColumns.SHOTS,
        UnderstatColumns.X_G_BUILDUP)
  }

  def dropNullAvgs(df: DataFrame): DataFrame = {
    df.na.drop(Seq(CalculatedColumns.BONUS_AVG))
      .orderBy(col(CommonColumns.DATE).asc)
  }
}
