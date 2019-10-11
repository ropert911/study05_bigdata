package com.xq.study.demo1hdfs.demo1hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;

public class HdfsUtilsImpl {
    public static void writeHdfsFile(String dhfsUrl, String filename, String content) {
        try {
            Configuration conf = new Configuration();
            HdfsUtilsImpl.configHdfs(conf, dhfsUrl);

            FileSystem fs = FileSystem.get(conf);
            Path path = new Path(filename);
            FSDataOutputStream fos = fs.create(path);

            //客户端设置副本数，最小1个
            fs.setReplication(path, (short) 1);
            fos.write(content.getBytes(), 0, content.length());
            fos.flush();
            fos.close();
            System.out.println("Create:" + filename);


            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void readHdfsFile(String dhfsUrl, String filename) {
        try {
            Configuration conf = new Configuration();
            HdfsUtilsImpl.configHdfs(conf, dhfsUrl);

            FileSystem fs = FileSystem.get(conf);
            Path file = new Path(filename);
            //判断文件是否存在
            if (!fs.exists(file)) {
                return;
            }

            FSDataInputStream getIt = fs.open(file);


            BufferedReader d = new BufferedReader(new InputStreamReader(getIt));
            String content = d.readLine();
            System.out.println("一行一行读取==============");
            while (null != content) {
                System.out.println(content);
                System.out.flush();
                content = d.readLine();
            }

            getIt.seek(0);
            OutputStream outputStream = new ByteArrayOutputStream(10240);
            IOUtils.copyBytes(getIt, outputStream, conf, false);
            System.out.println("用IOUtils.copyBytes读取===================");
            System.out.print(outputStream.toString());

            /**关闭文件*/
            d.close();
            /**关闭hdfs*/
            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void configHdfs(Configuration conf, String hdfsUrl) {
        //要以root方式登录，默认是本机用户名 sk-qianxiao， 权限不足
        System.setProperty("HADOOP_USER_NAME", "root");

        //使用主机名进行访问，要不然hdfs返回的就是内网ip
        conf.set("dfs.client.use.datanode.hostname", "true");
        conf.set("fs.defaultFS", hdfsUrl);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        //设置失败重试次数, 默认3次
        conf.set("dfs.client.max.block.acquire.failures", "1");
    }
}
