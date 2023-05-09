package unifiedData

import RollingAverage.applyRollingAvg
import constants.{CommonColumns, GameweekColumns, UnderstatColumns}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import unifiedData.UnifiedDataHelper.{dropColumnsAfterAvg, dropColumnsAfterJoin, dropNullAvgs}

class UnifiedDataProvider(gameweekFilteredDf: DataFrame, understatFilteredDf: DataFrame) {

  def getData: DataFrame = {
    val joinedDf = joinData(Seq(CommonColumns.NAME, CommonColumns.DATE))
    val unionedDf = joinDataWebNames(joinedDf)
    val joinedDfRollingAvg = applyRollingAvg(unionedDf, 5)
    val filteredColumnsDf = dropColumnsAfterAvg(joinedDfRollingAvg).orderBy(CommonColumns.DATE)
    dropNullAvgs(filteredColumnsDf)
  }

  private def joinData(columns: Seq[String]): DataFrame = {
    this.gameweekFilteredDf.join(this.understatFilteredDf, columns, "left_outer")
  }

  private def joinDataWebNames(joinedDf: DataFrame): DataFrame = {
    val nonNullDf = joinedDf.na.drop(Seq(UnderstatColumns.NPX_G))
    val nullDf = joinedDf.filter(joinedDf(UnderstatColumns.NPX_G).isNull)
    val nullDfDroppedCols = dropColumnsAfterJoin(nullDf)

    val understatRenamedDf = this.understatFilteredDf.withColumnRenamed(CommonColumns.NAME, UnderstatColumns.UNDERSTAT_NAME)
      .withColumnRenamed(CommonColumns.DATE, UnderstatColumns.UNDERSTAT_DATE)

    val nullJoinedDf = nullDfDroppedCols.join(understatRenamedDf,
      nullDfDroppedCols(GameweekColumns.WEB_NAME) === understatRenamedDf(UnderstatColumns.UNDERSTAT_NAME)
        && nullDfDroppedCols(CommonColumns.DATE) === understatRenamedDf(UnderstatColumns.UNDERSTAT_DATE),
      "left_outer"
    ).drop(UnderstatColumns.UNDERSTAT_NAME, UnderstatColumns.UNDERSTAT_DATE)

    nonNullDf.union(nullJoinedDf).orderBy(col(CommonColumns.DATE).asc) //.na.fill(0)
  }

  private def joinDataSurname(joinedDf: DataFrame): DataFrame = {
    val nonNullDf = joinedDf.na.drop(Seq(UnderstatColumns.NPX_G))
    val nullDf = joinedDf.filter(joinedDf(UnderstatColumns.NPX_G).isNull)
    val nullDfDroppedCols = dropColumnsAfterJoin(nullDf)

    val understatRenamedDf = this.understatFilteredDf.withColumnRenamed(CommonColumns.NAME, UnderstatColumns.UNDERSTAT_NAME)
      .withColumnRenamed(CommonColumns.DATE, UnderstatColumns.UNDERSTAT_DATE)

    val nullJoinedDf = nullDfDroppedCols.join(understatRenamedDf,
      nullDfDroppedCols("surname") === understatRenamedDf(UnderstatColumns.UNDERSTAT_NAME)
        && nullDfDroppedCols(CommonColumns.DATE) === understatRenamedDf(UnderstatColumns.UNDERSTAT_DATE),
      "left_outer"
    ).drop(UnderstatColumns.UNDERSTAT_NAME, UnderstatColumns.UNDERSTAT_DATE)

    nonNullDf.union(nullJoinedDf).orderBy(col(CommonColumns.DATE).asc) //.na.fill(0)
  }
}
