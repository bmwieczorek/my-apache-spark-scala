package com.bawi.spark

import java.io._
import java.nio.file.{Files, Paths}

import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf

object JobUtils {

  def deleteLocalDirectory(pathName: String): Unit = {
      val file = new File(pathName)
      if (file.exists())
        FileUtils.deleteDirectory(file)
  }

  def isLocal: Boolean = {
    isMac || isUbuntu
  }

  def isMac: Boolean = {
    if (System.getProperty("os.name").contains("Mac")) true else false
  }

  def isUbuntu: Boolean = {
    if (System.getProperty("os.name").contains("Linux")){
      val path = Paths.get ("/etc/os-release")
      if (path.toFile.exists()) {
        new String(Files.readAllBytes(path)).toLowerCase.contains("Ubuntu")
      } else false
    } else false
  }

}
