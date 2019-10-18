package com.study.scala.examples.streaming_kafka

import java.lang

import org.apache.kafka.common.serialization.BytesDeserializer
import org.apache.kafka.common.utils.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author sk-qianxiao
  * @date 2019/10/18
  */
object KafkaStream {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")

    val cmd = OptionUtil.parseArgs(args)


    //获取参数列表
    val appName = cmd.getOptionValue("app-name", this.getClass.getSimpleName).trim
    val kafkaBootstrapServers = cmd.getOptionValue("bootstrap-servers", "localhost:9092").trim  //kafka地址
    val kafkaListenGroupId = cmd.getOptionValue("group-id", "fire_safe_data_monitor").trim      //kafka监听时的 group
    val interval = cmd.getOptionValue("interval", "300").trim
    val topics = cmd.getOptionValue("topics", "itc-flume-hdfs-channel").trim
    val maxRatePerPartition = cmd.getOptionValue("max-rate", "2000").trim
    val isCluster = cmd.getOptionValue("is-cluster", "false").trim
    val checkpointPath = cmd.getOptionValue("checkpoint-path", "/spark/checkpoint/data_monitor/firesafe_checkpoint_0001").trim

    //创建StreamingContext
    def createContext(): StreamingContext = {
      createSparkStreamingContext(appName, kafkaBootstrapServers, kafkaListenGroupId, interval, topics, maxRatePerPartition, isCluster, checkpointPath)
    }

    //是否启用checkpoint
    if (isCluster.equals("true")) {
      val ssc = StreamingContext.getOrCreate(checkpointPath, createContext _)
      ssc.start()
      ssc.awaitTermination()
    } else {
      val ssc = createContext()
      ssc.start()
      ssc.awaitTermination()
    }
  }


  def createSparkStreamingContext(appName: String, kafkaBootstrapServers: String, groupId: String, interval: String,
                                  topics: String, maxRatePerPartition: String, isCluster: String,
                                  checkpointPath: String): StreamingContext = {
    //设置spark参数
    val conf = new SparkConf().setAppName(appName)
    //设置spark每秒最大从kafka每个partition分区获取数据速度
    conf.set("spark.streaming.kafka.maxRatePerPartition", maxRatePerPartition)
    //以本地模式运行,仅测试用,本地测试在VM参数中加入-DisLocal=true
    val isLocal = System.getProperty("isLocal")
    if (isLocal != null && isLocal.equals("true")) {
      conf.setMaster("local[2]")
    }

    val spark = SparkSession
      .builder()
      .appName(appName)
      .config(conf)
      //.config("spark.sql.warehouse.dir", "hdfs://ism:9000/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    //show databases
    spark.sql("show databases").show

    //StreamingContext,里面包含SparkContext
    val ssc = new StreamingContext(spark.sparkContext, Seconds(interval.toInt))

    //仅在真实集群环境中使用checkpoint
    if (isCluster.equals("true")) {
      //设置checkpoint目录
      ssc.checkpoint(checkpointPath)
    }

    //获取executor instances数量,用于自定义分区
    val executorCores = spark.sqlContext.getConf("spark.executor.cores", "2").toInt
    val executorNum = spark.sqlContext.getConf("spark.executor.instances", "1").toInt
    val partitions = executorCores * executorNum

    //不能加分号“;”......
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql(s"set spark.sql.shuffle.partitions=$partitions")

    //输出当前运行参数
    println(
      s"""
         |input params:
         |appName:$appName
         |bootstrapServers:$kafkaBootstrapServers
         |topics:$topics
         |groupId:$groupId
         |maxRatePerPartition:$maxRatePerPartition
         |interval:$interval
         |isCluster:$isCluster
         |checkpointPath:$checkpointPath
         |partitions:$partitions
        """.stripMargin)

    //kafka consumer sets
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafkaBootstrapServers,
      "key.deserializer" -> classOf[BytesDeserializer],
      "value.deserializer" -> classOf[BytesDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: lang.Boolean)
    )

    //同时获取多个topic数据
    val topicAry = topics.split(",")

    val dstream = KafkaUtils.createDirectStream[Bytes, Bytes](
      ssc,
      PreferConsistent,
      Subscribe[Bytes, Bytes](topicAry, kafkaParams)
    )

    //后面就进行Dstream的数据操作


    ssc
  }
}
