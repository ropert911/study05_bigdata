import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SQLContext


val deviceType = "3001"
val currentDay = "20200211"

val rowKeyStart = deviceType + "_" + currentDay + "_" + "0000000000000000"
val rowKeyStop = deviceType + "_" + currentDay + "_" + "FFFFFFFFFFFFFFFF"

val hBaseConf = HBaseConfiguration.create()
hBaseConf.set(TableInputFormat.INPUT_TABLE,"sensor_statistics_202002")
hBaseConf.set(TableInputFormat.SCAN_ROW_START, rowKeyStart)
hBaseConf.set(TableInputFormat.SCAN_ROW_STOP, rowKeyStop)
val hbaseRDD = sc.newAPIHadoopRDD(hBaseConf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])


val sqlContext = new SQLContext(sc)
import sqlContext.implicits._

val sinfos = hbaseRDD.map(r=>(
  Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("devid"))),
  Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("tmax"))),
  Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("tmin"))),
  Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("tcount"))),

  Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("hnmax"))),
  Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("hnmin"))),
  Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("hncount"))),

  Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("wlmax"))),
  Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("wlmin"))),
  Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("wlcount"))),

  Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("wpmax"))),
  Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("wpmin"))),
  Bytes.toString(r._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("wpcount")))
)).toDF("devid","tmax","tmin","tcount","hnmax","hnmin","hncount","wlmax","wlmin","wlcount","wpmax","wpmin","wpcount")
sinfos.registerTempTable("sinfos")
val dataframe = sqlContext.sql("SELECT tmax-tmin, tcount FROM sinfos")
val co =dataframe.collect
co.foreach(println)
