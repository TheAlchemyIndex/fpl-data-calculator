package understat

import org.apache.spark.sql.DataFrame
import understat.UnderstatHelper.{dropColumns, splitSurname}
import util.DataFrameHelper.renameColumns

class UnderstatProvider(understatDf: DataFrame) {

  def getData: DataFrame = {
    val camelCaseDf = renameColumns(understatDf)
    val splitSurnameDf: DataFrame = splitSurname(camelCaseDf)
    dropColumns(splitSurnameDf)
  }
}
