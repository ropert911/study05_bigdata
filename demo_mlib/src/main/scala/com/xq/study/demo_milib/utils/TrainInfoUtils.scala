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
  val deviceAlarmTypePathPatten = basePath + File.separator + "${deviceType}" + File.separator + "${alarmType}"
  val timePathPatten = deviceAlarmTypePathPatten + File.separator + "${time}"
  val modelFolder = "model"
  val infoFileName = "info"

  def getDeviceAlarmTypeFullPath(deviceType: String, alarmType: String): String = {
    var fullPath = deviceAlarmTypePathPatten.replaceAll("\\$\\{deviceType\\}", deviceType)
    fullPath = fullPath.replaceAll("\\$\\{alarmType\\}", alarmType)
    fullPath
  }

  def getTimeFullPath(deviceType: String, alarmType: String): String = {
    val sj = new SimpleDateFormat("yyyyMMddHH")
    val time = sj.format(Calendar.getInstance.getTime)

    var fullPath = timePathPatten.replaceAll("\\$\\{deviceType\\}", deviceType)
    fullPath = fullPath.replaceAll("\\$\\{alarmType\\}", alarmType)
    fullPath = fullPath.replaceAll("\\$\\{time\\}", time)
    fullPath
  }

  def getTrainModelPath(deviceType: String, alarmType: String): String = {
    val modelPath = getTimeFullPath(deviceType, alarmType) + File.separator + modelFolder
    modelPath
  }

  def getOtherInfoPath(deviceType: String, alarmType: String): String = {
    val infoPath = getTimeFullPath(deviceType, alarmType) + File.separator + infoFileName
    infoPath
  }

  def getOtherInfo(sc: SparkContext, deviceType: String, alarmType: String): Int = {
    val infoPath = getOtherInfoPath(deviceType, alarmType)
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

  def saveOtherInfo(sc: SparkContext, deviceType: String, alarmType: String, abNormalClusterIndex: Int) = {
    val infoPath = getOtherInfoPath(deviceType, alarmType)
    val array = Array(abNormalClusterIndex)
    val rdd = sc.parallelize(array)
    rdd.saveAsTextFile(infoPath)
  }

  def delTypeFolder(sc: SparkContext, deviceType: String, alarmType: String): Unit = {
    val basePath = getDeviceAlarmTypeFullPath(deviceType, alarmType)
    val path = new Path(basePath)
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)
    if (fileSystem.exists(path)) {
      fileSystem.delete(path, true)
    }
  }
}
