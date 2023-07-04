package writers

import com.github.mrpowers.spark.daria.sql.SparkSessionExt.SparkSessionMethods
import helpers.TestHelper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.scalatest.BeforeAndAfterAll
import writers.FileWriterTestSchema.fileWriterTestStruct

import scala.reflect.io.File

class FileWriterTest extends TestHelper with BeforeAndAfterAll {

  var TEMP_DIRECTORY: String = "src/test/resources/temp"
  var TARGET_DIRECTORY: String = "src/test/resources"
  var FILE_TYPE: String = "csv"
  var FILE_NAME: String = "test.csv"
  var FILE_PATH: String = s"$TARGET_DIRECTORY/$FILE_NAME"
  var FILE: File = File(FILE_PATH)

  var TEST_DF: DataFrame = SPARK.createDF(
    List(
      ("value1", 1, 1),
      ("value2", 2, 2),
      ("value3", 3, 3)
    ), List(
      ("col1", StringType, true),
      ("col2", IntegerType, true),
      ("col3", IntegerType, true)
    )
  )

  override def afterAll(): Unit = {
    FILE.delete()
  }

  test("writeToFile - It should create a new file in TARGET_DIRECTORY and delete TEMP_DIRECTORY") {
    val fileWriter: FileWriter = new FileWriter("csv", TARGET_DIRECTORY)
    fileWriter.writeToFile(TEST_DF, FILE_NAME)

    assert(FILE.exists)
    assert(FILE.isFile)
    assert(!File(TEMP_DIRECTORY).exists)
  }

  test("writeToFile - It should create a new file in TARGET_DIRECTORY with valid data") {
    val fileWriter: FileWriter = new FileWriter("csv", TARGET_DIRECTORY)
    fileWriter.writeToFile(TEST_DF, FILE_NAME)

    val fromFileDf: DataFrame = SPARK.read
      .option("header", value = true)
      .schema(fileWriterTestStruct)
      .csv(FILE_PATH)

    assert(TEST_DF.schema === fromFileDf.schema)
    assert(TEST_DF.collect().sameElements(fromFileDf.collect()))
  }
}
