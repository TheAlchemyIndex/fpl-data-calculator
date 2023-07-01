package writers

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import org.apache.spark.sql.DataFrame

import java.io.File
import java.nio.file.{Files, StandardCopyOption}

class FileWriter(fileType: String, baseFilePath: String) {

  val LOGGER: Logger = Logger(LoggerFactory.getLogger(getClass.getName))
  val TEMP_FILE_LOCATION: String = "data/temp"

  def writeToFile(df: DataFrame, newFileName: String): Unit = {
    df
      .coalesce(1)
      .write
      .option("header", "true")
      .option("delimiter", ",")
      .option("path", TEMP_FILE_LOCATION)
      .format(fileType)
      .save()

    moveAndDeleteFiles(TEMP_FILE_LOCATION, newFileName)
  }

  private def moveAndDeleteFiles(sourceLocation: String, newFileName: String): Unit = {
    val sourceDirectory: File = new File(sourceLocation)
    val targetDirectory: File = new File(this.baseFilePath)
    val sourceFileType: Option[File] = sourceDirectory.listFiles().find(_.getName.endsWith("." + fileType))

    sourceFileType match {
      case Some(csvFile) =>
        val newFilePath = new File(targetDirectory, newFileName).toPath
        val movedFile = csvFile.toPath
        Files.move(movedFile, newFilePath, StandardCopyOption.REPLACE_EXISTING)
        LOGGER.info(s"File [$newFileName] created and moved to ${targetDirectory.getAbsolutePath}.")
      case None =>
        LOGGER.warn(s"No file found in ${sourceDirectory.getAbsolutePath}.")
    }

    deleteDirectory(sourceDirectory)
  }

  private def deleteDirectory(directory: File): Unit = {
    if (directory.exists() && directory.isDirectory) {
      directory.listFiles().foreach(file => file.delete())
      directory.delete()
    } else {
      LOGGER.warn(s"${directory.getAbsolutePath} does not exist.")
    }
  }
}
