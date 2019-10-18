package com.study.scala.examples

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

import scala.util.parsing.json.JSON

/**
  * 这里的示例是spark-shell上运行过的示例
  *
  * @author xq
  * @data 2019/10/18
  **/
object RDDExamples_shell {
  /**
    * 构建RDD操作
    *
    * @param sc
    */
  def makeRDDExamples(sc: SparkContext): Unit = {
    //RDD[String]
    {
      //文件中的每一行成为一个元素 RDD[String]
      val lineRDD = sc.textFile("profile")
      //为RDD中的每个元素调用函数
      lineRDD.foreach(println)
    }

    //RDD[INT]
    {
      val array = Array(1, 2, 3, 4, 5, 6)
      //数组转RDD[INT]
      val rdd = sc.parallelize(array)
      rdd.foreach(println)

      //Map操作
      rdd.map(i => i * 2).foreach(println)
    }

    //键值对RDD, 注意用的是flatMap，map会生成RDD[Array[String]]
    {
      val lines = sc.textFile("profile")
      val pairRDD1 = lines.flatMap(line => line.split(" ")).map(word => (word, 1))
      //reduceByKey
      //sortByKey
      //mapValues对map的value做操作
      pairRDD1.reduceByKey(_ + _).sortByKey().mapValues(_ * 2).foreach(println)
    }

    //map的join操作
    {
      val pairRDD1 = sc.parallelize(Array(("spark", 1), ("spark", 2), ("hadoop", 3), ("hadoop", 5)))
      val pairRDD2 = sc.parallelize(Array(("spark", "fast")))
      pairRDD1.join(pairRDD2)
      pairRDD1.join(pairRDD2).foreach(println)
    }
  }

  /**
    * RDD转换，行动类 操作
    *
    * @param sc
    */
  def translateRDDExamples(sc: SparkContext): Unit = {
    {
      //文件中的每一行成为一个元素 RDD[String]
      val lineRDD = sc.textFile("profile")
      //为RDD中的每个元素调用函数
      lineRDD.foreach(println)
      //返回String first【行动操作】
      println(lineRDD.first())
      //返回Array[String] take【行动操作】
      lineRDD.take(3).foreach(println)


      //过滤操作 + 计数操作  count【行动操作】
      lineRDD.filter(line => line.contains("id")).count()
      lineRDD.filter(line => line.contains("id")).foreach(println)
    }

    //Map和 FlatMap
    {
      val array = Array(1, 2, 3, 4, 5, 6)
      //数组转RDD[INT]
      val rdd = sc.parallelize(array)
      //Map操作
      rdd.map(i => i * 2).foreach(println)
    }

    //Map和 FlatMap
    {
      val arr = sc.parallelize(Array(("A", 1), ("B", 2), ("C", 3)))
      //下面的 map输出A1, B2,C3  map转出来是RDD[String]
      arr.map(x => (x._1 + x._2)).foreach(println)
      //下面输出的是A,1,B,2,C,3, flatMap是转为RDD[CHAR]
      arr.flatMap(x => (x._1 + x._2)).foreach(println)
    }

    //groupByKey, reduceByKey ,collect
    {
      val list = List("hadoop", "spark", "hive", "spark", "spark")
      val rdd = sc.parallelize(list)
      //转出来是RDD[元组]
      val pairRdd = rdd.map(x => (x, 1))
      pairRdd.groupByKey().collect.foreach(println)
      //下面两个语句效果一样. reduceByKey 是值的操作
      pairRdd.reduceByKey(_ + _).collect.foreach(println)
      //groupByKey结果是：RDD[(String, scala.Iterable[Int])] map后是RDD[(String,Int)]
      // collect 是转化为数组的形式返回结果【行动操作】  Array[(String,Int)]
      pairRdd.groupByKey().map(t => (t._1, t._2.sum)).foreach(println)

      pairRdd.groupByKey().map(t => (t._1, t._2.sum)).collect().foreach(println)

      //返回String
      println(rdd.reduce((i1, i2) => i1 + i2))
    }
  }

  /**
    * 缓存操作，防止每次从数据源取数据
    *
    * @param sc
    */
  def rddCached(sc: SparkContext): Unit = {
    val list = List("Hadoop", "Spark", "Hive")

    val rdd = sc.parallelize(list)
    //会调用persist(MEMORY_ONLY)，但是，语句执行到这里，并不会缓存rdd，这是rdd还没有被计算生成
    rdd.cache()
    //和rdd.cache()一样
    //    rdd.persist()
    //第一次行动操作，触发一次真正从头到尾的计算，这时才会执行上面的rdd.cache()，把这个rdd放到缓存中
    println(rdd.count())
    //第二次行动操作，不需要触发从头到尾的计算，只需要重复使用上面缓存中的rdd
    println(rdd.collect().mkString(","))
    rdd.unpersist()
  }

  /**
    * 分区，重新分区
    *
    * @param sc
    */
  def partition(sc: SparkContext): Unit = {
    val array = Array(1, 2, 3, 4, 5)
    //设置两个分区
    val rdd = sc.parallelize(array, 2)
    rdd.repartition(4)
  }

  @volatile private var shareData: Broadcast[Int] = null

  /**
    * 共享变量：在本机共享的
    *
    * @param sc
    */
  def shareData(sc: SparkContext): Unit = {
    val a = 3;
    //生成共享变量
    synchronized {
      shareData = sc.broadcast(a)
    }

    //更新
    shareData.unpersist(true)
    shareData = sc.broadcast(4)

    //使用
    val data = shareData.value
  }

  /**
    * 累加器
    *
    * @param sc
    */
  def abc(sc: SparkContext): Unit = {
    {
      val accum = sc.longAccumulator("My Accumulator")
      sc.parallelize(Array(1, 2, 3, 4)).foreach(x => accum.add(x))
      accum.value
    }
    {
      val accum = sc.doubleAccumulator("My Accumulator")
      sc.parallelize(Array(1.1, 2.1, 3.1, 4.1)).foreach(x => accum.add(x))
      accum.value
    }
  }

  /**
    * 文件操作
    *
    * @param sc
    */
  def fileOperation(sc: SparkContext): Unit = {
    //三个是一样的
    val textFile1 = sc.textFile("hdfs://localhost:9000/user/hadoop/word.txt")
    val textFile2 = sc.textFile("/user/hadoop/word.txt")
    val textFile3 = sc.textFile("word.txt")
    textFile3.first()
    textFile3.saveAsTextFile("file:///usr/local/spark/mycode/wordcount/writeback.txt")
  }


  /**
    * Json处理
    */
  def jsonOperation(sc: SparkContext): Unit = {
    val inputFile = "file:///opt/spark-2.3.4-bin-hadoop2.7/examples/src/main/resources/people.json"
    sc.textFile(inputFile).foreach(println)

    val result = sc.textFile(inputFile).map(s => JSON.parseFull(s))
    result.foreach({ r =>
      r match {
        case Some(map: Map[String, Any]) => println(map)
        case None => println("Parsing failed")
        case other => println("Unknown data structure: " + other)
      }
    })
  }
}
