package providers

import org.apache.spark.sql.DataFrame

trait Provider {
  def getData: DataFrame
}
