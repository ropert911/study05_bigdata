package com.xq.study.demo_milib.utils

import java.io.File
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext

/**
  * @author sk-qianxiao
  * @date 2019/10/25
  */
object TrainInfoUtils {
  val basePath = "C:\\Users\\sk-qianxiao\\Desktop\\data\\traininfo"
  val typePathPatten = basePath + File.separator + "${deviceType}"
  val timePathPatten = typePathPatten + File.separator + "${time}"
  val modelFolder = "model"
  val infoFileName = "info"

  def getTypeFullPath(deviceType: String): String = {
    val typeFullPath = typePathPatten.replaceAll("\\$\\{deviceType\\}", deviceType)
    typeFullPath
  }
  def getTimeFullPath(deviceType: String): String = {
    val sj = new SimpleDateFormat("yyyyMMddHH")
    val time = sj.format(Calendar.getInstance.getTime)
    //    calendar.add(Calendar.DATE, -1)

    var basePath = timePathPatten.replaceAll("\\$\\{deviceType\\}", deviceType)
    basePath = basePath.replaceAll("\\$\\{time\\}", time)
    basePath
  }

  def getTrainModelPath(deviceType: String): String = {
    val modelPath = getTimeFullPath(deviceType) + File.separator + modelFolder
    modelPath
  }

  def getOtherInfoPath(deviceType: String): String = {
    val infoPath = getTimeFullPath(deviceType) + File.separator + infoFileName
    infoPath
  }

  def getOtherInfo(sc: SparkContext, deviceType: String): Int = {
    val infoPath = getOtherInfoPath(deviceType)
    val txtRdd = sc.textFile(infoPath)
    try {
      val index = txtRdd.first().toInt
      index
    } catch {
      case ex: Exception => {
        -1
      }
    }
  }

  def saveOtherInfo(sc: SparkContext, deviceType: String, abNormalClusterIndex: Int) = {
    val infoPath = getOtherInfoPath(deviceType)
    val array = Array(abNormalClusterIndex)
    val rdd = sc.parallelize(array)
    rdd.saveAsTextFile(infoPath)
  }

  def delTypeFolder(sc: SparkContext, deviceType: String): Unit = {
    val basePath = getTypeFullPath(deviceType)
    val path = new Path(basePath)
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)
    if (fileSystem.exists(path)) {
      fileSystem.delete(path, true)
    }
  }
}
