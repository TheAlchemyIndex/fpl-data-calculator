package providers.understat.players

import constants.UnderstatPlayersColumns
import org.apache.spark.sql.DataFrame
import providers.Provider
import util.DataFrameHelper.renameColumnsToCamelCase

class UnderstatPlayersProvider(understatDf: DataFrame) extends Provider {

  def getData: DataFrame = {
    val camelCaseDf: DataFrame = renameColumnsToCamelCase(understatDf)
    dropColumns(camelCaseDf)
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
