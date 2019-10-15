package com.xq.study.hive.demo1hive;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * Created By sk-tengfeiwang on 2019/7/13.
 */
@Component
public class HiveDriver {
    private static final String driverName = "org.apache.hive.jdbc.HiveDriver";

    @Value("jdbc:hive2://node1:10000/iot")
    private String url;

    @Value("root")
    private String user;

    public Connection getConnection() throws Exception {
        Class.forName(driverName);
        Connection conn = DriverManager.getConnection(url, user, "");
        return conn;
    }
}
