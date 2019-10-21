package com.study.scala

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 从mysql读取数据，形成DataFrame
  *
  * @author sk-qianxiao
  * @date 2019/10/17
  */
object batchDFFromMysql {
  val mysql_url = "jdbc:mysql://node1:3306/hive?useSSL=false"
  val mysql_user = "root"
  val mysql_password = "123456"

  def main(args: Array[String]) {
    //设置用户名是防止用户没有权限访问集群，以及hdfs
    System.setProperty("user.name", "root")
    System.setProperty("HADOOP_USER_NAME", "root")

    val spark = SparkSession.builder().appName("batchDFFromMysql_AppName").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    //方法一 =========
    def read4mysql(table: String): DataFrame = spark.read.format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", mysql_url)
      .option("user", mysql_user)
      .option("password", mysql_password)
      .option("dbtable", table)
      .load()
    //        println("================DBS")
    //        val order_df = read4mysql("DBS")
    println("================(select * from DBS) as tt")
    val order_df = read4mysql("(select * from DBS) as tt")

    //方法二 ==============
    //定义Propertites，确定链接MySQL的参数
    //    val mysqlProperties = new java.util.Properties()
    //    mysqlProperties.put("driver", "com.mysql.jdbc.Driver") //确定driver
    //    mysqlProperties.put("user", mysql_user) //用户名
    //    mysqlProperties.put("password", mysql_password) //密码
    //    mysqlProperties.put("fetchsize", "10000") //批次取数数量
    //    mysqlProperties.put("lowerBound", "1") //确定分区
    //    mysqlProperties.put("upperBound", "3") //确定分区
    //    mysqlProperties.put("numPartitions", "2") //分区数量
    //    mysqlProperties.put("partitionColumn", "DB_ID") //分区字段
    ////    println("================(select * from DBS) as tt")
    ////    val mysqlTableName = "(select * from DBS) as tt"
    //    println("================DBS")
    //    val mysqlTableName = "DBS"
    //    val order_df = spark.read.jdbc(mysql_url, mysqlTableName, mysqlProperties)


    order_df.show(10)
    spark.close()
  }
}