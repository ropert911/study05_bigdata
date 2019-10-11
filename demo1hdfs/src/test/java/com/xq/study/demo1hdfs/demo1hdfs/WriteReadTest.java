package com.xq.study.demo1hdfs.demo1hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class WriteReadTest {

    public static void main(String[] args) {
        try {
            System.setProperty("HADOOP_USER_NAME","root");

            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://node1:9000");
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");


            byte[] buff = "Hello world".getBytes(); // 要写入的内容

            String filename = "test"; //要写入的文件名
            FileSystem fs = FileSystem.get(conf);
            Path path = new Path(filename);
            FSDataOutputStream os = fs.create(path);
            //客户端设置副本数，最小1个
            fs.setReplication(path, (short)2);
            os.write(buff, 0, buff.length);
            System.out.println("Create:" + filename);


            os.close();
            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
