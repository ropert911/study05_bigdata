package com.xq.study.hive.demo1hive;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
public class Demo1hiveApplicationTests {
    Logger logger = LoggerFactory.getLogger(Demo1hiveApplicationTests.class);
    @Autowired
    private HiveDriver hiveDriver;

    @Test
    public void test1() {
        try {
            Connection conn = hiveDriver.getConnection();
            Statement statement = conn.createStatement();

            List<String> sqlsList = new ArrayList<>();
            sqlsList.add("create database if not exists iot");
            sqlsList.add("create table if not exists iot.device_alarm(" +
                    "       deviceid string comment 'device id'," +
                    "       date_info bigint comment '日期信息，精确到时分秒等'," +
                    "       data_type int comment '数据类型'," +
                    "       first_areaid bigint comment '一级区域ID'," +
                    "       second_areaid bigint comment '二级区域ID'," +
                    "       third_areaid bigint comment '三级区域ID'," +
                    "       device_type string comment 'sensor type'," +
                    "       total_alarm int comment '告警总数'," +
                    "       critical int comment '严重个数'," +
                    "       major int comment '重要个数'," +
                    "       normal int comment '一般个数'," +
                    "       warn int comment '告警个数'" +
                    ")" +
                    "partitioned by (datefmt int comment '分区字段，day')" +
                    "row format delimited fields terminated by '^' " +
                    "stored as textfile");
            for (String sql : sqlsList) {
                boolean result = statement.execute(sql);
                logger.info("sql : {} , status : {}", sql, result);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
