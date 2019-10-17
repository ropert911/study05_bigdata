package com.study.scala.examples

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by sk-qianxiao on 2019/3/21.
  */
object wordcount {
  def main(args: Array[String]) {
    //hadoop用到的:如果不设置，就默认就是sk-qianxiao，没有权限的
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    //spark用到的
    System.setProperty("user.name", "hdfs")
    //    val conf = new SparkConf().setAppName("Simple Application").setMaster("local").set("dfs.client.use.datanode.hostname", "true")
    val conf = new SparkConf().setAppName("Simple Application").set("dfs.client.use.datanode.hostname", "true")
      //      .setMaster("local")
      //      .setMaster('spark://10.21.208.21:7077')
//      .setMaster("mesos://host:port")
      .setMaster("yarn-cluster")
    //    .setMaster("yarn-client")
    //使用主机名进行访问，要不然hdfs返回的就是内网ip
    //    conf.set("dfs.client.use.datanode.hostname", "true")
    //    conf.set("dfs.datanode.use.datanode.hostname", "true")


    val sc = new SparkContext(conf)


    val logFile = "hdfs://node1:9000/profile"
    val rdd = sc.textFile(logFile)
    println("=================内容==============")
    rdd.foreach(println)

    //    println("=================word count==============")
    //    val wordcount = rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).map(x => (x._2, x._1)).sortByKey(false).map(x => (x._2, x._1))
    //    wordcount.foreach(println)
    //    wordcount.saveAsTextFile("hdfs://node1:9000/result")

    sc.stop()
  }







//  val spark = SparkSession
//    .builder()
//    .appName("SparkSQLApp")
//    .master("local[2]")
//    .config("dfs.client.use.datanode.hostname", "true")
//    .getOrCreate()
}
