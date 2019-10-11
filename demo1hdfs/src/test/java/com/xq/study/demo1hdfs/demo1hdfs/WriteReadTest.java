package com.xq.study.demo1hdfs.demo1hdfs;

public class WriteReadTest {
    private static String HDFS_URL = "hdfs://node1:9000";

    public static void main(String[] args) {
        HdfsUtilsImpl.writeHdfsFile(HDFS_URL, "test", "Xq write content22222222222");
        HdfsUtilsImpl.readHdfsFile(HDFS_URL, "test");
    }
}
