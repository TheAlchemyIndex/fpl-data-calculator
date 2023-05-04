package writers

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import org.apache.spark.sql.DataFrame

import java.io.File
import java.nio.file.{Files, StandardCopyOption}

class FileWriter(sourceLocation: String, targetLocation: String, fileType: String) {

  val LOGGER: Logger = Logger(LoggerFactory.getLogger(getClass.getName))

  def writeToFile(df: DataFrame): Unit = {
    df
      .coalesce(1)
      .write
      .option("header", "true")
      .option("delimiter", ",")
      .option("path", sourceLocation)
      .format(fileType)
      .save()

    moveAndDeleteFiles()
  }

  private def moveAndDeleteFiles(): Unit = {
    val sourceDirectory: File = new File(sourceLocation)
    val targetDirectory: File = new File(targetLocation)
    val fileType: Option[File] = sourceDirectory.listFiles().find(_.getName.endsWith(".csv"))

    fileType match {
      case Some(csvFile) =>
        val newFileName = "calculated_data.csv"
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
