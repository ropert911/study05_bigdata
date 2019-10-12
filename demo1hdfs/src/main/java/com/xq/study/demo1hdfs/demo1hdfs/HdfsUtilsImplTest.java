package com.xq.study.demo1hdfs.demo1hdfs;

import com.xq.study.demo1hdfs.demo1hdfs.utils.HdfsUtilsImpl;

/**
 * @author sk-qianxiao
 */
public class HdfsUtilsImplTest {
    private static String HDFS_URL = "hdfs://node1:9000";

    public static void main(String[] args) {
        String filename = "test";
        HdfsUtilsImpl.writeHdfsFile(HDFS_URL, filename, "Xq write content 111 \n");
        HdfsUtilsImpl.readHdfsFile(HDFS_URL, filename);
        HdfsUtilsImpl.rmHdfsFile(HDFS_URL, filename);
    }
}
