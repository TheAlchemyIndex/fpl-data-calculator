package gameweek

import DateFormatter.formatDate
import constants.GameweekColumns
import gameweek.GameweekHelper.{booleanColumnToBinary, dropColumns}
import org.apache.spark.sql.DataFrame
import util.DataFrameHelper.renameColumns

class GameweekProvider(gameweekDf: DataFrame) {

  def getData: DataFrame = {
    val camelCaseDf = renameColumns(gameweekDf)
    val homeFixtureToBinaryDf = booleanColumnToBinary(camelCaseDf, GameweekColumns.HOME_FIXTURE, GameweekColumns.WAS_HOME)
    val formattedDateDf = formatDate(homeFixtureToBinaryDf)
    dropColumns(formattedDateDf)
  }
}
