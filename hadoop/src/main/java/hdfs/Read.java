package hdfs;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.IOUtils;

/**
 * Created by sk-qianxiao on 2018/8/13.
 */
public class Read {
    public static void main(String[] args) {
        try {
            System.setProperty("HADOOP_USER_NAME","root");

            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://node1:9000");
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            //设置失败重试次数, 默认3次
            conf.set("dfs.client.max.block.acquire.failures", "1");

            FileSystem fs = FileSystem.get(conf);
            Path file = new Path("/user/root/input/kms-acls.xml");

            //判断文件是否存在
            if (!fs.exists(file)) {
                return;
            }

            FSDataInputStream getIt = fs.open(file);

            BufferedReader d = new BufferedReader(new InputStreamReader(getIt));
            String content = d.readLine(); //读取文件一行
            while (null != content) {
                System.out.println(content);
                System.out.flush();
                content = d.readLine();
            }

            System.out.println("===================用IOUtils.copyBytes读取");
            getIt.seek(0);
            OutputStream outputStream = new ByteArrayOutputStream(10240);
            IOUtils.copyBytes(getIt, outputStream, conf, false);
            System.out.print(outputStream.toString());
            d.close(); //关闭文件
            fs.close(); //关闭hdfs
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

