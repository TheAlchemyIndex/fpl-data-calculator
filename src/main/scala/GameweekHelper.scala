import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, when}

object GameweekHelper {

  def booleanColumnToBinary(df: DataFrame, newColumn: String, targetColumn: String): DataFrame = {
    df.withColumn(newColumn,
      when(col(targetColumn) === true, 1).otherwise(0))
  }

  def dropUnnecessaryColumns(df: DataFrame): DataFrame = {
    df
      .drop("transfers_balance",
        "own_goals",
        "kickoff_time",
        "red_cards",
        "element",
        "bps",
        "was_home",
        "penalties_missed",
        "fixture")
  }

  def dropColumnsAfterAvg(df: DataFrame): DataFrame = {
    df
      .drop("bonus",
        "clean_sheets",
        "goals_conceded",
        "team_a_score",
        "influence",
        "transfers_in",
        "saves",
        "assists",
        "creativity",
        "value",
        "selected",
        "goals_scored",
        "minutes",
        "yellow_cards",
        "transfers_out",
        "threat",
        "ict_index",
        "penalties_saved",
        "team_h_score")
  }
}
