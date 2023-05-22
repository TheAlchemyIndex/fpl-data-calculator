package writers

import com.github.mrpowers.spark.daria.sql.SparkSessionExt.SparkSessionMethods
import helpers.TestHelper
import helpers.schemas.fileWriter.FileWriterTestSchema.fileWriterTestStruct
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.scalatest.BeforeAndAfterAll

import scala.reflect.io.File

class FileWriterTests extends TestHelper with BeforeAndAfterAll {

  val SOURCE_DIRECTORY: String = "src/test/resources/temp"
  val TARGET_DIRECTORY: String = "src/test/resources"
  val FILE_PATH: String = TARGET_DIRECTORY + "/calculated_data.csv"
  val FILE: File = File(FILE_PATH)

  val TEST_DF: DataFrame = SPARK.createDF(
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

//  test("writeToFile - It should create a new file in the targetDirectory of FileWriter and delete the sourceDirectory") {
//    val fileWriter: FileWriter = new FileWriter(SOURCE_DIRECTORY, TARGET_DIRECTORY, "csv")
//    fileWriter.writeToFile(TEST_DF)
//
//    assert(FILE.exists)
//    assert(FILE.isFile)
//    assert(!File(SOURCE_DIRECTORY).exists)
//  }
//
//  test("writeToFile - It should create a new file in the targetDirectory of FileWriter with valid data") {
//    val fileWriter: FileWriter = new FileWriter(SOURCE_DIRECTORY, TARGET_DIRECTORY, "csv")
//    fileWriter.writeToFile(TEST_DF)
//
//    val fromFileDf: DataFrame = SPARK.read
//      .option("header", value = true)
//      .schema(fileWriterTestStruct)
//      .csv(FILE_PATH)
//
//    assert(TEST_DF.schema === fromFileDf.schema)
//    assert(TEST_DF.collect().sameElements(fromFileDf.collect()))
//  }
}
