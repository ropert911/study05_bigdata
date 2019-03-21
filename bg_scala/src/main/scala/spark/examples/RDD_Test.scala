package spark.examples

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

/**
  * Created by sk-qianxiao on 2018/8/24.
  */

object RDD_Test {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)

    fileOp(sc);
    jsonOp(sc)
    numberOp(sc);
    test4(sc);    //关系操作

    sc.stop();
  }

  def fileOp(sc: SparkContext) {
    //这里默认就是hdfs的地址
    val inputFile = "/user/root/input/hdfs-site.xml"
    val textFile = sc.textFile(inputFile)
    textFile.first() //获取第一行
    textFile.saveAsTextFile("file:///usr/local/spark/mycode/wordcount/writeback.txt") //另存为

    val pairRDD = textFile.flatMap(line => line.split(" ")).map(word => (word, 1))
    pairRDD.partitionBy(new org.apache.spark.HashPartitioner(2))  //根据特定方式分区
    pairRDD.repartition(4);   //重新分区

    pairRDD.groupByKey().foreach(println) //groupByKey：应用于(K,V)键值对的数据集时，返回一个新的(K, Iterable)形式的数据集
    pairRDD.reduceByKey((a, b) => a + b) //reduceByKey根据key做分组 review，参数是value
    pairRDD.sortByKey().foreach(println) //返回一个根据键排序的RDD
    pairRDD.mapValues(x => x + 1).foreach(println) //对value部分进行转换
    pairRDD.foreach(println)
    pairRDD.keys.foreach(println)
    pairRDD.values.foreach(println)
  }

  def jsonOp(sc: SparkContext): Unit = {
    val jsonStr = sc.textFile("file:///opt/spark-2.3.1-bin-hadoop2.7/examples/src/main/resources/people.json")
    jsonStr.foreach(println)

    val result = jsonStr.map(s => JSON.parseFull(s))
    result.foreach({ r =>
      r match {
        case Some(map: Map[String, Any]) => println(map)
        case None => println("Parsing failed")
        case other => println("Unknown data structure: " + other)
      }
    })
  }

  def numberOp(sc: SparkContext): Unit = {
    val array = Array(1, 2, 3, 4, 5)
    val rdd1 = sc.parallelize(array)

    val rdd2 = sc.parallelize(1 to 10, 2) //设置2个分区
    val rdd3 = rdd2.filter(x => 0 == x % 2) //filter:筛选出满足函数func的元素，并返回一个新的数据集
      .map(_ * 2) //对RDD中的每个元素都乘于2  //map: 将每个元素传递到函数func中，并将结果返回为一个新的数据集
      .flatMap(x => (1 to x)) //与map类似，但每个元素输入项都可以被映射到0个或多个的输出项，最终将结果”扁平化“后输出

    val map2 = rdd3.repartition(3) //重新分区

    rdd3.cache() //会调用persist(MEMORY_ONLY)
    rdd3.persist()
    rdd3.unpersist() //去持久化

    rdd3.count();
    rdd3.foreach(println)
    rdd3.foreach(x => print(x + " "))
    rdd3.take(100).foreach(println) //task:提取部分
    rdd3.reduce((a, b) => if (a > b) a else b) //reduce 取最大值

    //累加器
    val accum = sc.longAccumulator("My Accumulator")
    rdd3.foreach(x => accum.add(x))
    accum.value
  }

  def test4(sc: SparkContext): Unit = {
    val pairRDD1 = sc.parallelize(Array(("spark", 1), ("spark", 2), ("hadoop", 3), ("hadoop", 5)))
    val pairRDD2 = sc.parallelize(Array(("spark", "fast")))

    pairRDD1.join(pairRDD2).foreach(println)

  }
}