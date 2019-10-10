package com.xq.study.hbase.hbase;

import com.xq.study.hbase.hbase.utils.HBaseUtils;
import com.xq.study.hbase.hbase.utils.impl.HBaseUtilsImpl;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Date;

@RunWith(SpringRunner.class)
@SpringBootTest
public class HBaseUtilsImplTests {

    @Test
    public void test1() {
        Connection connection = HBaseUtilsImpl.getConnection("47.105.32.179:2181,47.104.181.73:2181,120.27.15.222:2181");
        System.out.println("After getConnection");

        HBaseUtilsImpl.getAdmin();
        System.out.println("After getAdmin");

        try {
            HBaseUtils hbu = new HBaseUtilsImpl();
            System.out.println("After  new HBaseUtilsImpl");
            hbu.getAllTables();
            System.out.println("After  getAllTables");

            //hbu.descTable("people");

            //String[] infos = {"info","family"};
            //hbu.createTable("people", infos);

            //String[] add = {"cs1","cs2"};
            //String[] remove = {"cf1","cf2"};

            //HColumnDescriptor hc = new HColumnDescriptor("sixsixsix");

            //hbu.modifyTable("stu",hc);
            //hbu.getAllTables();

//            hbu.putData("huoying", "rk001", "cs2", "name", "aobama", new Date().getTime());
//            hbu.getAllTables();

            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
