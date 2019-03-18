package spark.examples

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

/**
  * Created by sk-qianxiao on 2018/8/24.
  */

object Hive_Test {
  def main(args: Array[String]) {
    case class Record(key: Int, value: String)

    val warehouseLocation = "spark-warehouse"
    val spark = SparkSession.builder().appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport().getOrCreate()

    reaTest(spark)
    writeTest(spark)
  }

  def reaTest(spark: SparkSession): Unit = {
    //下面是运行结果
    spark.sql("SELECT * FROM sparktest.student").show()
  }

  def writeTest(spark: SparkSession): Unit = {
    //下面我们设置两条数据表示两个学生信息
    val studentRDD = spark.sparkContext.parallelize(Array("3 Rongcheng M 26", "4 Guanhua M 27")).map(_.split(" "))

    //下面要设置模式信息
    val schema = StructType(List(StructField("id", IntegerType, true), StructField("name", StringType, true), StructField("gender", StringType, true), StructField("age", IntegerType, true)))

    //下面创建Row对象，每个Row对象都是rowRDD中的一行
    val rowRDD = studentRDD.map(p => Row(p(0).toInt, p(1).trim, p(2).trim, p(3).toInt))

    //建立起Row对象和模式之间的对应关系，也就是把数据和模式对应起来
    val studentDF = spark.createDataFrame(rowRDD, schema)

    //查看studentDF
    studentDF.show()
    //下面注册临时表
    studentDF.createOrReplaceTempView("tempTable")
    spark.sql("insert into sparktest.student select * from tempTable")
  }
}