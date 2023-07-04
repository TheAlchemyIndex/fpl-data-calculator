package providers.impl.understat.players

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, to_date}
import providers.Provider
import util.constants.{CommonColumns, UnderstatPlayersColumns, UnderstatTeamsColumns}

class UnderstatPlayersProvider(understatDf: DataFrame) extends Provider {

  def getData: DataFrame = {
    dropColumns(understatDf)
      .withColumn(CommonColumns.DATE, to_date(col(UnderstatTeamsColumns.DATE), "yyyy-MM-dd"))
  }

  private def dropColumns(df: DataFrame): DataFrame = {
    df.drop(UnderstatPlayersColumns.X_G_CHAIN,
        UnderstatPlayersColumns.H_GOALS,
        UnderstatPlayersColumns.A_TEAM,
        UnderstatPlayersColumns.ROSTER_ID,
        UnderstatPlayersColumns.ASSISTS,
        UnderstatPlayersColumns.SEASON,
        UnderstatPlayersColumns.A_GOALS,
        UnderstatPlayersColumns.TIME,
        UnderstatPlayersColumns.POSITION,
        UnderstatPlayersColumns.ID,
        UnderstatPlayersColumns.H_TEAM,
        UnderstatPlayersColumns.GOALS)
  }
}
