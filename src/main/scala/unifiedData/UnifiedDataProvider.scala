package unifiedData

import RollingAverage.applyRollingAvg
import constants.CommonColumns
import org.apache.spark.sql.DataFrame
import unifiedData.UnifiedDataHelper.{dropColumnsAfterAvg, dropNullAvgs}

class UnifiedDataProvider(gameweekFilteredDf: DataFrame, understatFilteredDf: DataFrame) {

  def getData: DataFrame = {
    val joinedDf = joinData(Seq(CommonColumns.NAME, CommonColumns.DATE))
    val joinedDfRollingAvg = applyRollingAvg(joinedDf, 5)
    val filteredColumnsDf = dropColumnsAfterAvg(joinedDfRollingAvg).orderBy(CommonColumns.DATE)
    dropNullAvgs(filteredColumnsDf).show()
    dropNullAvgs(filteredColumnsDf)
  }

  def joinData(columns: Seq[String]): DataFrame = {
    this.gameweekFilteredDf.join(this.understatFilteredDf, columns, "left_outer")
  }
}
