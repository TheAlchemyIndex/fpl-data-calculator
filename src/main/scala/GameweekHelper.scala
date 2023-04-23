import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, when}

object GameweekHelper {

  def booleanColumnToBinary(gameweeksDf: DataFrame, newColumn: String, targetColumn: String): DataFrame = {
    gameweeksDf.withColumn(newColumn, when(col(targetColumn) === true, 1).otherwise(0))
  }

  def dropExtraColumns(gameweeksDf: DataFrame): DataFrame = {
    gameweeksDf
      .drop("transfers_balance",
        "own_goals",
        "red_cards",
        "element",
        "bps",
        "was_home",
        "penalties_missed",
        "fixture")
  }
}
