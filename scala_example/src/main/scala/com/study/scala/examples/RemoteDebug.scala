package com.study.scala

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author sk-qianxiao
  * @date 2019/10/17
  */
object RemoteDebug {

 //设置用户名是防止用户没有权限访问集群，以及hdfs
 System.setProperty("user.name", "root")
 System.setProperty("HADOOP_USER_NAME", "root")


 val spark: SparkSession = SparkSession.builder()
   .appName("remote_test")
   .master("spark://master_ip:master_port")
   .config("spark.jars","F:\\IdeaProject\\project\\target\\package.jar")
   .config("spark.driver.host", "192.168.*.*")
   .config("spark.driver.port", "9089")

   .enableHiveSupport().getOrCreate()
 val sc: SparkContext = spark.sparkContext
 sc.setLogLevel("ERROR")

 val mysql_url =  "jdbc:mysql://server_ip:server_port/instance?useSSL=false"
 val mysql_user = "username"
 val mysql_password = "password"

 def read4mysql(table:String):DataFrame= spark.read.format("jdbc")
   .option("driver", "com.mysql.jdbc.Driver")
   .option("url",mysql_url)
   .option("user",mysql_user)
   .option("password",mysql_password)
   .option("dbtable",table)
   .load()
 def main(args: Array[String]) {

  val order_df = read4mysql(
   s"(select * from a ")

  order_df.show(10)

  spark.close()
 }
}