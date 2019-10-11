package com.xq.study.hbase.hbase;

import com.xq.study.hbase.hbase.utils.HBaseUtils;
import com.xq.study.hbase.hbase.utils.impl.HBaseUtilsImpl;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Date;
import java.util.Iterator;

public class HBaseUtilsImplTests {
    public static void main(String[] args) {
        Connection connection = HBaseUtilsImpl.getConnection("node1:2181,node2:2181,node3:2181");
        HBaseUtilsImpl.getAdmin();

        try {
            HBaseUtils hbu = new HBaseUtilsImpl();
            System.out.println("查询有哪些表项=======");
            hbu.getAllTables();

            System.out.println("建表=======");
            String[] infos = {"info", "family"};
            hbu.createTable("people", infos);
            System.out.println("查看表的描述=======");
            hbu.descTable("people");

            System.out.println("添加列簇=======");
            String[] infos1 = {"info1", "family2"};
            hbu.modifyTable("people", infos1);
            System.out.println("查看表的描述=======");
            hbu.descTable("people");

            System.out.println("添加&删除列簇=======");
            String[] infos2Add = {};
            String[] infos2Del = {"info1", "family2"};
            hbu.modifyTable("people", infos2Add, infos2Del);
            System.out.println("查看表的描述=======");
            hbu.descTable("people");

            System.out.println("不存在的情况下添加列簇=======");
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("info");
            hbu.modifyTable("people", hColumnDescriptor);
            hColumnDescriptor = new HColumnDescriptor("info1");
            hbu.modifyTable("people", hColumnDescriptor);
            System.out.println("查看表的描述=======");
            hbu.descTable("people");

            System.out.println("enable表=======");
            hbu.enableTable("people");


            System.out.println("添加数据=======");
            hbu.putData("people", "r00001", "family", "name", "肖家", new Date().getTime());
            hbu.putData("people", "r00002", "family", "name", "王家");
            hbu.putData("people", "r00003", "family", "name", "王家");
            hbu.putData("people", "r00004", "family", "name", "王家");
            hbu.putData("people", "r00005", "family", "name", "王家");
            System.out.println("删除数据=======");
            hbu.deleteColumn("people", "r00003");
            hbu.deleteColumn("people", "r00004", "family");
            hbu.deleteColumn("people", "r00005", "family", "name");


            System.out.println("查询数据=======");
            Result result1 = hbu.getResult("people", "r00001");
            Result result2 = hbu.getResult("people", "r00001", "family");
            Result result3 = hbu.getResult("people", "r00001", "family", "name");
            byte[] name1 = result1.getValue(Bytes.toBytes("family"), Bytes.toBytes("name"));
            byte[] name2 = result2.getValue(Bytes.toBytes("family"), Bytes.toBytes("name"));
            byte[] name3 = result3.getValue(Bytes.toBytes("family"), Bytes.toBytes("name"));
            System.out.println(name1);
            System.out.println(name2);
            System.out.println(name3);

            System.out.println("扫描全表=======");
            ResultScanner resultScanner = hbu.getResultScann("people");
            Iterator<Result> iterator = resultScanner.iterator();
            while (iterator.hasNext()) {
                Result resultT = iterator.next();
                byte[] nameT = resultT.getValue(Bytes.toBytes("family"), Bytes.toBytes("name"));
                System.out.println(Bytes.toString(nameT));
            }

            System.out.println("查看表是否存在=====" + hbu.existTable("people"));
            System.out.println("Disable表=====");
            hbu.disableTable("people");

            System.out.println("删除表=======");
            hbu.dropTable("people");

            System.out.println("查询有哪些表项=======");
            hbu.getAllTables();

            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
