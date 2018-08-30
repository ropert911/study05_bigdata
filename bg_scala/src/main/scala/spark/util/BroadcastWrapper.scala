package spark.util

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.slf4j.LoggerFactory
import java.util
import scala.collection.immutable.Map

/**
  * 获取或更新boradcast共享数据
  */
object BroadcastWrapper {
  val logger = LoggerFactory.getLogger(BroadcastWrapper.getClass)

  @volatile private var apAreaInstance: Broadcast[Map[java.lang.String, java.lang.Long]] = null

  /**
    * 更新共享变量信息
    *
    * @param sc
    * @param ismServer
    * @param innerPort
    */
  def update(sc: SparkContext, ismServer: String, innerPort: String): Unit = {
    import scala.collection.JavaConverters._
    val apArea = (new util.HashMap[java.lang.String, java.lang.Long]()).asScala.toMap
    if (apArea != null) {
      //删除老的广播变量
      if (apAreaInstance != null)
        apAreaInstance.unpersist(true)

      //更新新的广播变量
      apAreaInstance = sc.broadcast(apArea)
      logger.info("update broadcast apAreaInstance success,size=" + apArea.size)
    } else {
      logger.error("getApArea data is null,please check...")
    }
  }

  /**
    * 获取共享变量信息
    *
    * @param sc
    * @param ismServer
    * @param innerPort
    * @return
    */
  def getApAreaInstance(sc: SparkContext, ismServer: String, innerPort: String): Broadcast[Map[java.lang.String, java.lang.Long]] = {
    if (apAreaInstance == null) {
      synchronized {
        if (apAreaInstance == null) {
          import scala.collection.JavaConverters._
          val apArea = (new util.HashMap[java.lang.String, java.lang.Long]()).asScala.toMap
          apAreaInstance = sc.broadcast(apArea)
        }
      }
    }
    apAreaInstance
  }
}
