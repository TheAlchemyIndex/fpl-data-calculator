package unifiedData

import constants.{GameweekColumns, UnderstatColumns}
import org.apache.spark.sql.DataFrame

object UnifiedDataHelper {

  def dropColumnsAfterAvg(df: DataFrame): DataFrame = {
    df
      .drop(GameweekColumns.BONUS,
        GameweekColumns.CLEAN_SHEETS,
        GameweekColumns.GOALS_CONCEDED,
        GameweekColumns.TOTAL_POINTS,
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
}
