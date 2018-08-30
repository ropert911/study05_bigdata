package spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by sk-qianxiao on 2018/8/30.
  */
object QueueStream {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("TestRDDQueue").setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(3))
    val rddQueue = new scala.collection.mutable.SynchronizedQueue[RDD[Int]]()
    val queueStream = ssc.queueStream(rddQueue)
    val mappedStream = queueStream.map(r => (r % 10, 1))
    val reducedStream = mappedStream.reduceByKey(_ + _)
    reducedStream.print()

    ssc.start()

    for (i <- 1 to 30) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 100, 2) //数据为1到100， 分2个分区
      Thread.sleep(1000)
    }

    ssc.stop()
  }
}
