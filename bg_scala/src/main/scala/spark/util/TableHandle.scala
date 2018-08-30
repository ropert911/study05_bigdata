package spark.util

import common.{TableType, Tables}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Spark Stream 不同数据处理入口
  * Created by sk-qianxiao on 2018/7/10.
  */
object TableHandle {
  /**
    * Spark Stream 不同数据处理入口函数
    *
    * @param spark
    * @param rdd
    * @param tableType
    * @return
    */
  def handleRecords(spark: SparkSession, rdd: RDD[Tables], tableType: Int) = {
//    tableType match {
//      case tableType if tableType == TableType.ClientAll.getTableType => ClientAllHandle.handlerClientAll(spark, rdd)
//      case tableType if tableType == TableType.SessionAll.getTableType => SessionAllHandle.handlerSessionAll(spark, rdd)
//      case tableType if tableType == TableType.SessionAssocAll.getTableType => SessionAssocAllHandle.handlerSessionAssocAll(spark, rdd)
//      case tableType if tableType == TableType.ApAll.getTableType => ApAllHandle.handlerApAll(spark, rdd)
//      case tableType if tableType == TableType.ApWanAll.getTableType => ApWanAllHandle.handlerApWanAll(spark, rdd)
//      case tableType if tableType == TableType.ApRadioAll.getTableType => ApRadioAllHandle.handlerApRadioAll(spark, rdd)
//      case tableType if tableType == TableType.ApVapInfoAll.getTableType => ApVapInfoAllHandle.handlerApVapInfoAll(spark, rdd)
//      case tableType if tableType == TableType.ApVapTrafficAll.getTableType => ApVapTrafficAllHandle.handlerApVapTrafficAll(spark, rdd)
//      case tableType if tableType == TableType.ApVapAssocAll.getTableType => ApVapAssocAllHandle.handlerApVapAssocAll(spark, rdd)
//      case tableType if tableType == TableType.ApVapSnrAll.getTableType => ApVapSnrAllHandle.handlerApVapSnrAll(spark, rdd)
//      case tableType if tableType == TableType.AcAll.getTableType => AcAllHandle.handlerAcAll(spark, rdd)
//      case tableType if tableType == TableType.ApOnacAll.getTableType => ApOnacAllHandle.handlerApOnacAll(spark, rdd)
//      case tableType if tableType == TableType.AcDhcpAll.getTableType => AcDhcpAllHandle.handlerAcDhcpAll(spark, rdd)
//      case tableType if tableType == TableType.AcInterfaceAll.getTableType => AcInterfaceAllHandle.handlerAcInterfaceAll(spark, rdd)
//      case _ => "unSupport..."
//    }
  }
}
