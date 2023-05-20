package understat

import org.apache.spark.sql.DataFrame
import understat.UnderstatTeamsHelper.dropColumns
import util.DataFrameHelper.renameColumns

class UnderstatTeamsProvider(understatTeamDf: DataFrame) {

  def getData: DataFrame = {
    val camelCaseDf = renameColumns(understatTeamDf)
    dropColumns(camelCaseDf)
  }
}
