package com.xq.study.hbase.hbase.utils;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

/**
 * @author sk-qianxiao
 */
public interface HBaseUtils {
    /**
     * 查询所有表
     *
     * @throws Exception
     */
    void getAllTables() throws Exception;

    /**
     * 创建表，传参，表名和列簇的名字
     *
     * @param tableName
     * @param family
     * @throws Exception
     */
    void createTable(String tableName, String[] family) throws Exception;

    /**
     * 创建表，传参:封装好的多个列簇
     *
     * @param htds
     * @throws Exception
     */
    void createTable(HTableDescriptor htds) throws Exception;

    /**
     * 创建表，传参，表名和封装好的多个列簇
     *
     * @param tableName
     * @param htds
     * @throws Exception
     */
    void createTable(String tableName, HTableDescriptor htds) throws Exception;

    /**
     * 查看表的列簇属性
     *
     * @param tableName
     * @throws Exception
     */
    void descTable(String tableName) throws Exception;

    /**
     * 判断表存在不存在
     *
     * @param tableName
     * @return
     * @throws Exception
     */
    boolean existTable(String tableName) throws Exception;

    /**
     * enable表
     *
     * @param tableName
     * @throws Exception
     */
    void enableTable(String tableName) throws Exception;

    /**
     * disable表
     *
     * @param tableName
     * @throws Exception
     */
    void disableTable(String tableName) throws Exception;

    /**
     * drop表
     *
     * @param tableName
     * @throws Exception
     */
    void dropTable(String tableName) throws Exception;

    /**
     * 修改表(增加和删除)
     *
     * @param tableName
     * @param familys
     * @throws Exception
     */
    void modifyTable(String tableName, String[] familys) throws Exception;

    /**
     * 修改表(增加和删除)
     *
     * @param tableName
     * @param addColumn
     * @param removeColumn
     * @throws Exception
     */
    void modifyTable(String tableName, String[] addColumn, String[] removeColumn) throws Exception;

    /**
     * 当列簇不存在时添加
     *
     * @param tableName
     * @param hcds
     * @throws Exception
     */
    void modifyTable(String tableName, HColumnDescriptor hcds) throws Exception;

    /**
     * 添加数据
     * tableName:    表明
     * rowKey:    行键
     * familyName:列簇
     * columnName:列名
     * value:        值
     */
    void putData(String tableName, String rowKey, String familyName, String columnName, String value) throws Exception;

    /**
     * 添加数据
     * @param tableName
     * @param rowKey
     * @param familyName
     * @param columnName
     * @param value
     * @param timestamp
     * @throws Exception
     */
    void putData(String tableName, String rowKey, String familyName, String columnName, String value, long timestamp) throws Exception;

    /**
     * 根据rowkey查询数据
     *
     * @param tableName
     * @param rowKey
     * @return
     * @throws Exception
     */
    Result getResult(String tableName, String rowKey) throws Exception;

    /**
     * 根据rowkey查询数据
     *
     * @param tableName
     * @param rowKey
     * @param familyName
     * @return
     * @throws Exception
     */
    Result getResult(String tableName, String rowKey, String familyName) throws Exception;

    /**
     * 根据rowkey查询数据
     *
     * @param tableName
     * @param rowKey
     * @param familyName
     * @param columnName
     * @return
     * @throws Exception
     */
    Result getResult(String tableName, String rowKey, String familyName, String columnName) throws Exception;

    /**
     * 查询指定version
     *
     * @param tableName
     * @param rowKey
     * @param familyName
     * @param columnName
     * @param versions
     * @return
     * @throws Exception
     */
    Result getResultByVersion(String tableName, String rowKey, String familyName, String columnName, int versions) throws Exception;

    /**
     * scan全表数据
     *
     * @param tableName
     * @return
     * @throws Exception
     */
    ResultScanner getResultScann(String tableName) throws Exception;

    /**
     * scan全表数据
     *
     * @param tableName
     * @param scan
     * @return
     * @throws Exception
     */
    ResultScanner getResultScann(String tableName, Scan scan) throws Exception;

    /**
     * 删除数据（指定的列）
     *
     * @param tableName
     * @param rowKey
     * @throws Exception
     */
    void deleteColumn(String tableName, String rowKey) throws Exception;

    /**
     * 删除数据（指定的列）
     *
     * @param tableName
     * @param rowKey
     * @param falilyName
     * @throws Exception
     */
    void deleteColumn(String tableName, String rowKey, String falilyName) throws Exception;

    /**
     * 删除数据（指定的列）
     *
     * @param tableName
     * @param rowKey
     * @param falilyName
     * @param columnName
     * @throws Exception
     */
    void deleteColumn(String tableName, String rowKey, String falilyName, String columnName) throws Exception;
}
