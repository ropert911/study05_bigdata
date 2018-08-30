package spark


import java.lang
import java.util.Calendar

import org.apache.kafka.common.serialization.BytesDeserializer
import org.apache.kafka.common.utils.Bytes
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.commons.cli.{GnuParser, HelpFormatter, Options}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory
import common.{GpbParser, TableType, TimeUtils}
import spark.util.{BroadcastWrapper, TableHandle}

/**
  * spark数据解析
  * Created By sk-tengfeiwang on 2018/6/11.
  */
object KafkaParser {
  val logger = LoggerFactory.getLogger(KafkaParser.getClass)
  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    //参数选项
    val options = getOptions()
    val parser = new GnuParser
    //参数解析
    val cmd = parser.parse(options, args)

    if (cmd.hasOption("help")) {
      usage(options)
      System.exit(0)
    }

    //获取参数列表
    val appName = cmd.getOptionValue("app-name", "DataParse").trim
    val bootstrapServers = cmd.getOptionValue("bootstrap-servers", "localhost:9092").trim
    val groupId = cmd.getOptionValue("group-id", "test_dp").trim
    val interval = cmd.getOptionValue("interval", "120").trim
    val gpbTopic = cmd.getOptionValue("gpb-topic", "pm_base").trim
    val maxRatePerPartition = cmd.getOptionValue("max-rate", "10000").trim
    val isCluster = cmd.getOptionValue("is-cluster", "false").trim
    val minute = cmd.getOptionValue("minute", "5").trim
    val ismServer = cmd.getOptionValue("ism-server", "").trim
    val ismInnerPort = cmd.getOptionValue("ism-innerport", "10015").trim
    val checkpointPath = cmd.getOptionValue("checkpoint-path", "/spark/checkpoint/data_parser/checkpoint_0000000000").trim

    //创建StreamingContext
    def createContext(): StreamingContext = {
      createSparkStreamingContext(appName, bootstrapServers, groupId, interval, gpbTopic, maxRatePerPartition, minute, isCluster, checkpointPath, ismServer, ismInnerPort)
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

  def createSparkStreamingContext(appName: String, bootstrapServers: String, groupId: String, interval: String,
                                  gpbTopic: String, maxRatePerPartition: String, minute: String, isCluster: String,
                                  checkpointPath: String, ismServer: String, ismInnerPort: String): StreamingContext = {
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
    spark.sql("show databases").show

    //获取executor instances数量,用于自定义分区
    val executorCores = spark.sqlContext.getConf("spark.executor.cores", "2").toInt
    val executorNum = spark.sqlContext.getConf("spark.executor.instances", "1").toInt
    val partitions = (executorCores - 1) * executorNum

    //不能加分号“;”......
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql(s"set spark.sql.shuffle.partitions=$partitions")

    //StreamingContext,里面包含SparkContext
    val ssc = new StreamingContext(spark.sparkContext, Seconds(interval.toInt))

    //仅在真实集群环境中使用checkpoint
    if (isCluster.equals("true")) {
      //设置checkpoint目录
      ssc.checkpoint(checkpointPath)
    }

    //输出当前运行参数
    println(s"input params:appName:$appName \n bootstrapServers:$bootstrapServers \n " +
      s"groupId:$groupId \n interval:$interval \n gpbTopic:$gpbTopic \n" +
      s"\n maxRatePerPartition:$maxRatePerPartition  \n partitions::$partitions \n ")

    //kafka consumer sets
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> bootstrapServers,
      "key.deserializer" -> classOf[BytesDeserializer],
      "value.deserializer" -> classOf[BytesDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: lang.Boolean)
    )

    //同时获取多个topic数据
    val topics = (gpbTopic.trim).split(",")

    try {
      val dstream = KafkaUtils.createDirectStream[Bytes, Bytes](
        ssc,
        PreferConsistent,
        Subscribe[Bytes, Bytes](topics, kafkaParams)
      )

      dstream.map(record => record.value().get()).repartition(partitions).flatMap(value => {
        import scala.collection.JavaConverters._
        GpbParser.parseData(value).asScala.toList
      }).map(record => (record.getTableType, record))
        .foreachRDD(rdd => {
          //更新共享变量
          BroadcastWrapper.update(rdd.sparkContext, ismServer, ismInnerPort)
          //获取共享变量
          val apArea = BroadcastWrapper.getApAreaInstance(rdd.sparkContext, ismServer, ismInnerPort).value

          val result = rdd.map(table => (table._1, {
            if (table._2.isAp) {
              val areaId = apArea.get(table._2.getDevMac)
              if (!areaId.isEmpty) {
                table._2.setTopAreaid(areaId.get.longValue())
              }
            }
            table._2
          }))

          result.persist()
          TableType.values().foreach(tableType => {
            val tableRecordRdd = result.filter(record => record._1 == tableType.getTableType)
              .map(record => record._2)
            TableHandle.handleRecords(spark, tableRecordRdd, tableType.getTableType)
          })

          result.unpersist()
        })

    } catch {
      case ex: Exception => logger.error("DataParser handler data error", ex)
      case t: Throwable => logger.error("unknow exception...", t)
    }

    ssc
  }


  /**
    * 将hive中数据按小时整合文件
    *
    * @param overwriteMinute
    * @param spark
    * @return
    */
  private def overwriteHiveDataByHour(overwriteMinute: String, spark: SparkSession, partition: Int) = {
    //整合数据，每小时每个分区一个文件
    val cal = Calendar.getInstance()
    val minute = cal.get(Calendar.MINUTE)
    cal.add(Calendar.HOUR_OF_DAY, -1)
    val dateFmt = TimeUtils.long2TimezoneStr(cal.getTimeInMillis, TimeUtils.DEFAULT_TIMEZONE, "yyyyMMdd").toInt
    if (minute == overwriteMinute.toInt) {
      val beginTime = System.currentTimeMillis()
      //overwrite sql...
      val overwriteSql = s"insert overwrite table pm.user_traffic partition(date_fmt,area_id) " +
        s"select * from pm.user_traffic where date_fmt=$dateFmt"
      //execute sql...
      spark.sql(overwriteSql)
      val endTime = System.currentTimeMillis()

      logger.info("overwrite cost time:" + (endTime - beginTime))
    } else {
      logger.info(s"overwrite dateFmt:$dateFmt false!!!\tbecause overwirteMinute per hour is $overwriteMinute per hour...")
    }
  }

  /**
    * 获取配置参数
    *
    * @return
    */
  def getOptions(): Options = {
    val options = new Options
    options.addOption("a", "app-name", true, "app name")
    options.addOption("i", "interval", true, "interval time (seconds)")
    options.addOption("b", "bootstrap-servers", true, "kafka broker list")
    options.addOption("g", "group-id", true, "kafka group id")
    options.addOption("t", "gpb-topic", true, "the topic list of gpb")
    options.addOption("m", "max-rate", true, "maxRatePerPartition or messages per second for each partiton")
    options.addOption("z", "is-cluster", true, "single node is false,cluster is true")
    options.addOption("c", "checkpoint-path", true, "StreamingContext checkpoint path")
    options.addOption("n", "minute", true, "minute per hour to overwrite data")
    options.addOption("s", "ism-server", true, "ism server ip address")
    options.addOption("p", "ism-innerport", true, "ism inner api port")
    options.addOption("h", "help", false, "help")
  }

  /**
    * 获取帮助信息
    *
    * @param opt
    */
  def usage(opt: Options): Unit = {
    val formatter = new HelpFormatter();
    formatter.printHelp("the information of bi-spark :", opt);
  }
}