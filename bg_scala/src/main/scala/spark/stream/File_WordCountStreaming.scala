package spark.stream

import org.apache.spark._
import org.apache.spark.streaming._

object File_WordCountStreaming  {
  def main(args: Array[String]) {
    //设置为本地运行模式，2个线程，一个监听，另一个处理数据
    val sparkConf = new SparkConf().setAppName("WordCountStreaming").setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(20))// 时间间隔为20秒
    val lines = ssc.textFileStream("file:///home/xiaoqian/logfile")  //这里采用本地文件，当然你也可以采用HDFS文件
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

//参考： http://dblab.xmu.edu.cn/blog/1385-2/
//Spark Streaming每隔20秒就监听一次。
// 但是，你这时会感到奇怪，既然启动监听了，为什么程序没有把我们刚才放置在”/usr/local/spark/mycode/streaming/logfile”
// 目录下的log1.txt和log2.txt这两个文件中的内容读取出来呢？
// 原因是，监听程序只监听”/usr/local/spark/mycode/streaming/logfile”目录下在程序启动后新增的文件，不会去处理历史上已经存在的文件。
// 所以，为了能够让程序读取文件内容并显示到屏幕上，让我们能够看到效果，这时，我们需要到”/usr/local/spark/mycode/streaming/logfile”
// 目录下再新建一个log3.txt文件，请打开另外一个终端窗口（我们称为shell窗口），当前正在执行监听工作的spark-shell窗口依然保留。
