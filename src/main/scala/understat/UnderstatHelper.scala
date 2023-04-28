package understat

import constants.UnderstatColumns
import org.apache.spark.sql.DataFrame

object UnderstatHelper {

  def dropColumns(df: DataFrame): DataFrame = {
    df
      .drop(UnderstatColumns.X_G_CHAIN,
        UnderstatColumns.H_GOALS,
        UnderstatColumns.A_TEAM,
        UnderstatColumns.ROSTER_ID,
        UnderstatColumns.ASSISTS,
        UnderstatColumns.SEASON,
        UnderstatColumns.A_GOALS,
        UnderstatColumns.TIME,
        UnderstatColumns.POSITION,
        UnderstatColumns.ID,
        UnderstatColumns.H_TEAM,
        UnderstatColumns.GOALS)
  }
}
