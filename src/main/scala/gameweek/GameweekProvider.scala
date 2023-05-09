package gameweek

import DateFormatter.formatDate
import constants.GameweekColumns
import gameweek.GameweekHelper.{booleanColumnToBinary, dropColumns, splitSurname}
import org.apache.spark.sql.{Column, DataFrame}
import util.DataFrameHelper.renameColumns

class GameweekProvider(gameweekDf: DataFrame) {

  def getData: DataFrame = {
    val camelCaseDf: DataFrame = renameColumns(gameweekDf)
    val homeFixtureToBinaryDf: DataFrame = booleanColumnToBinary(camelCaseDf, GameweekColumns.HOME_FIXTURE, GameweekColumns.WAS_HOME)
    val formattedDateDf: DataFrame = formatDate(homeFixtureToBinaryDf)
    val splitSurnameDf: DataFrame = splitSurname(formattedDateDf)
    dropColumns(splitSurnameDf)
  }
}
