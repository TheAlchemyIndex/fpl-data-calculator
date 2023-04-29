package helpers

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class TestHelper extends AnyFunSuite {

  val SPARK: SparkSession = SparkSession.builder()
    .appName("FPL Data Calculator - Test")
    .master("local[*]")
    .getOrCreate()
}
