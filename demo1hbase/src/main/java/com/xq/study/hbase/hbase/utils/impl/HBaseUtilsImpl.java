package com.xq.study.hbase.hbase.utils.impl;

import com.xq.study.hbase.hbase.utils.HBaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;


/**
 * @author sk-qianxiao
 */
public class HBaseUtilsImpl implements HBaseUtils {
    private static final String ZK_CONNECT_KEY = "hbase.zookeeper.quorum";

    private static Connection conn = null;
    private static Admin admin = null;


    /**
     * 获取连接
     *
     * @param zkConnectValue
     * @return
     */
    public static Connection getConnection(String zkConnectValue) {
        // 创建一个可以用来管理hbase配置信息的conf对象
        Configuration conf = HBaseConfiguration.create();
        // 设置当前的程序去寻找的hbase在哪里
        conf.set(ZK_CONNECT_KEY, zkConnectValue);
        //另外的方式就是指定hdfs_core,hdfs_site,hbase_site配置文件
//        conf.addResource("/etc/hbase/conf/core-site.xml");
//        conf.addResource("/etc/hbase/conf/hdfs-site.xml");
//        conf.addResource("/etc/hbase/conf/hbase-site.xml");
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * 获取管理员对象
     *
     * @return
     */
    public static Admin getAdmin() {
        try {
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return admin;
    }

    @Override
    public void getAllTables() throws Exception {
        //获取列簇的描述信息
        HTableDescriptor[] listTables = admin.listTables();
        for (HTableDescriptor listTable : listTables) {
            //转化为表名
            String tbName = listTable.getNameAsString();
            //获取列的描述信息
            HColumnDescriptor[] columnFamilies = listTable.getColumnFamilies();
            System.out.println("tableName:" + tbName);
            for (HColumnDescriptor columnFamilie : columnFamilies) {
                //获取列簇的名字
                String columnFamilyName = columnFamilie.getNameAsString();
                System.out.print("\t" + "columnFamilyName:" + columnFamilyName);
            }
            System.out.println();
        }
    }

    @Override
    public void createTable(String tableName, String[] family) throws Exception {
        TableName name = TableName.valueOf(tableName);
        //判断表是否存在
        if (admin.tableExists(name)) {
            System.out.println("table已经存在！");
        } else {
            //表的列簇示例
            HTableDescriptor htd = new HTableDescriptor(name);
            //向列簇中添加列的信息
            for (String str : family) {
                HColumnDescriptor hcd = new HColumnDescriptor(str);
                htd.addFamily(hcd);
            }
            //创建表
            admin.createTable(htd);
            //判断表是否创建成功
            if (admin.tableExists(name)) {
                System.out.println("table创建成功");
            } else {
                System.out.println("table创建失败");
            }
        }
    }

    @Override
    public void createTable(HTableDescriptor htds) throws Exception {
        //获得表的名字
        String tbName = htds.getNameAsString();
        admin.createTable(htds);
    }

    @Override
    public void createTable(String tableName, HTableDescriptor htds) throws Exception {
        TableName name = TableName.valueOf(tableName);

        if (admin.tableExists(name)) {
            System.out.println("table已经存在！");
        } else {
            admin.createTable(htds);
            boolean flag = admin.tableExists(name);
            System.out.println(flag ? "创建成功" : "创建失败");
        }
    }


    @Override
    public void descTable(String tableName) throws Exception {
        //转化为表名
        TableName name = TableName.valueOf(tableName);
        //判断表是否存在
        if (admin.tableExists(name)) {
            //获取表中列簇的描述信息
            HTableDescriptor tableDescriptor = admin.getTableDescriptor(name);
            //获取列簇中列的信息
            HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
            for (HColumnDescriptor columnFamily : columnFamilies) {
                System.out.println(columnFamily);
            }
        } else {
            System.out.println("table不存在");
        }
    }

    @Override
    public boolean existTable(String tableName) throws Exception {
        TableName name = TableName.valueOf(tableName);
        return admin.tableExists(name);
    }

    @Override
    public void enableTable(String tableName) throws Exception {
        TableName name = TableName.valueOf(tableName);

        if (admin.tableExists(name)) {
            if (admin.isTableDisabled(name)) {
                admin.enableTable(name);
            } else {
                System.out.println("table是活动状态");
            }
        } else {
            System.out.println("table不存在");
        }
    }

    @Override
    public void disableTable(String tableName) throws Exception {
        TableName name = TableName.valueOf(tableName);

        if (admin.tableExists(name)) {
            if (admin.isTableEnabled(name)) {
                admin.disableTable(name);
            } else {
                System.out.println("table不是活动状态");
            }
        } else {
            System.out.println("table不存在");
        }
    }

    @Override
    public void dropTable(String tableName) throws Exception {
        //转化为表名
        TableName name = TableName.valueOf(tableName);
        //判断表是否存在
        if (admin.tableExists(name)) {
            //判断表是否处于可用状态
            boolean tableEnabled = admin.isTableEnabled(name);

            if (tableEnabled) {
                //使表变成不可用状态
                admin.disableTable(name);
            }
            //删除表
            admin.deleteTable(name);
            //判断表是否存在
            if (admin.tableExists(name)) {
                System.out.println("删除失败");
            } else {
                System.out.println("删除成功");
            }

        } else {
            System.out.println("table不存在");
        }
    }

    @Override
    public void modifyTable(String tableName, String[] familys) throws Exception {
        //转化为表名
        TableName name = TableName.valueOf(tableName);
        //判断表是否存在
        if (admin.tableExists(name)) {
            //判断表是否可用状态
            boolean tableEnabled = admin.isTableEnabled(name);

            if (tableEnabled) {
                //使表变成不可用
                admin.disableTable(name);
            }
            //根据表名得到表
            HTableDescriptor tableDescriptor = admin.getTableDescriptor(name);
            for (String str : familys) {
                tableDescriptor.addFamily(new HColumnDescriptor(str.getBytes()));
            }

            //替换该表所有的列簇
            admin.modifyTable(name, tableDescriptor);
        } else {
            System.out.println("table不存在");
        }
    }

    @Override
    public void modifyTable(String tableName, String[] addColumn, String[] removeColumn) throws Exception {
        //转化为表名
        TableName name = TableName.valueOf(tableName);
        //判断表是否存在
        if (admin.tableExists(name)) {
            //判断表是否可用状态
            boolean tableEnabled = admin.isTableEnabled(name);

            if (tableEnabled) {
                //使表变成不可用
                admin.disableTable(name);
            }
            //根据表名得到表
            HTableDescriptor tableDescriptor = admin.getTableDescriptor(name);
            //创建列簇结构对象，添加列
            for (String add : addColumn) {
                HColumnDescriptor addColumnDescriptor = new HColumnDescriptor(add);
                tableDescriptor.addFamily(addColumnDescriptor);
            }
            //创建列簇结构对象，删除列
            for (String remove : removeColumn) {
                HColumnDescriptor removeColumnDescriptor = new HColumnDescriptor(remove);
                tableDescriptor.removeFamily(removeColumnDescriptor.getName());
            }

            admin.modifyTable(name, tableDescriptor);
        } else {
            System.out.println("table不存在");
        }
    }

    @Override
    public void modifyTable(String tableName, HColumnDescriptor hcds) throws Exception {
        //转化为表名
        TableName name = TableName.valueOf(tableName);
        //根据表名得到表
        HTableDescriptor tableDescriptor = admin.getTableDescriptor(name);
        //获取表中所有的列簇信息
        HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();

        boolean flag = false;
        //判断参数中传入的列簇是否已经在表中存在
        for (HColumnDescriptor columnFamily : columnFamilies) {
            if (columnFamily.equals(hcds)) {
                flag = true;
            }
        }
        //存在提示，不存在直接添加该列簇信息
        if (flag) {
            System.out.println("该列簇已经存在");
        } else {
            tableDescriptor.addFamily(hcds);
            admin.modifyTable(name, tableDescriptor);
        }
    }


    @Override
    public void putData(String tableName, String rowKey, String familyName, String columnName, String value)
            throws Exception {
        //转化为表名
        TableName name = TableName.valueOf(tableName);
        //添加数据之前先判断表是否存在，不存在的话先创建表
        if (admin.tableExists(name)) {

        } else {
            //根据表明创建表结构
            HTableDescriptor tableDescriptor = new HTableDescriptor(name);
            //定义列簇的名字
            HColumnDescriptor columnFamilyName = new HColumnDescriptor(familyName);
            tableDescriptor.addFamily(columnFamilyName);
            admin.createTable(tableDescriptor);

        }

        Table table = conn.getTable(name);
        Put put = new Put(rowKey.getBytes());

        put.addColumn(familyName.getBytes(), columnName.getBytes(), value.getBytes());
        table.put(put);
    }

    @Override
    public void putData(String tableName, String rowKey, String familyName, String columnName, String value,
                        long timestamp) throws Exception {
        // 转化为表名
        TableName name = TableName.valueOf(tableName);
        // 添加数据之前先判断表是否存在，不存在的话先创建表
        if (admin.tableExists(name)) {

        } else {
            // 根据表明创建表结构
            HTableDescriptor tableDescriptor = new HTableDescriptor(name);
            // 定义列簇的名字
            HColumnDescriptor columnFamilyName = new HColumnDescriptor(familyName);
            tableDescriptor.addFamily(columnFamilyName);
            admin.createTable(tableDescriptor);

        }

        Table table = conn.getTable(name);
        Put put = new Put(rowKey.getBytes());

        //put.addColumn(familyName.getBytes(), columnName.getBytes(), value.getBytes());
        put.addImmutable(familyName.getBytes(), columnName.getBytes(), timestamp, value.getBytes());
        table.put(put);
    }


    @Override
    public Result getResult(String tableName, String rowKey) throws Exception {
        Result result;
        TableName name = TableName.valueOf(tableName);
        if (admin.tableExists(name)) {
            Table table = conn.getTable(name);
            Get get = new Get(rowKey.getBytes());
            result = table.get(get);
        } else {
            result = null;
        }
        return result;
    }

    @Override
    public Result getResult(String tableName, String rowKey, String familyName) throws Exception {
        Result result;
        TableName name = TableName.valueOf(tableName);
        if (admin.tableExists(name)) {
            Table table = conn.getTable(name);
            Get get = new Get(rowKey.getBytes());
            get.addFamily(familyName.getBytes());
            result = table.get(get);

        } else {
            result = null;
        }
        return result;
    }

    @Override
    public Result getResult(String tableName, String rowKey, String familyName, String columnName) throws Exception {
        Result result;
        TableName name = TableName.valueOf(tableName);
        if (admin.tableExists(name)) {
            Table table = conn.getTable(name);
            Get get = new Get(rowKey.getBytes());
            get.addColumn(familyName.getBytes(), columnName.getBytes());
            result = table.get(get);
        } else {
            result = null;
        }
        return result;
    }

    @Override
    public Result getResultByVersion(String tableName, String rowKey, String familyName, String columnName,
                                     int versions) throws Exception {
        Result result;
        TableName name = TableName.valueOf(tableName);
        if (admin.tableExists(name)) {
            Table table = conn.getTable(name);
            Get get = new Get(rowKey.getBytes());
            get.addColumn(familyName.getBytes(), columnName.getBytes());
            get.setMaxVersions(versions);
            result = table.get(get);
        } else {
            result = null;
        }
        return result;
    }


    @Override
    public ResultScanner getResultScann(String tableName) throws Exception {
        ResultScanner result;
        TableName name = TableName.valueOf(tableName);
        if (admin.tableExists(name)) {
            Table table = conn.getTable(name);
            Scan scan = new Scan();
            result = table.getScanner(scan);
        } else {
            result = null;
        }
        return result;
    }

    @Override
    public ResultScanner getResultScann(String tableName, Scan scan) throws Exception {
        ResultScanner result;
        TableName name = TableName.valueOf(tableName);
        if (admin.tableExists(name)) {
            Table table = conn.getTable(name);
            result = table.getScanner(scan);
        } else {
            result = null;
        }
        return result;
    }

    @Override
    public void deleteColumn(String tableName, String rowKey) throws Exception {
        TableName name = TableName.valueOf(tableName);
        if (admin.tableExists(name)) {
            Table table = conn.getTable(name);
            Delete delete = new Delete(rowKey.getBytes());
            table.delete(delete);
        } else {
            System.out.println("table不存在");
        }
    }


    @Override
    public void deleteColumn(String tableName, String rowKey, String falilyName) throws Exception {
        TableName name = TableName.valueOf(tableName);
        if (admin.tableExists(name)) {
            Table table = conn.getTable(name);
            Delete delete = new Delete(rowKey.getBytes());
            delete.addFamily(falilyName.getBytes());
            table.delete(delete);
        } else {
            System.out.println("table不存在");
        }
    }

    @Override
    public void deleteColumn(String tableName, String rowKey, String falilyName, String columnName) throws Exception {
        TableName name = TableName.valueOf(tableName);
        if (admin.tableExists(name)) {
            Table table = conn.getTable(name);
            Delete delete = new Delete(rowKey.getBytes());
            delete.addColumn(falilyName.getBytes(), columnName.getBytes());
            table.delete(delete);
        } else {
            System.out.println("table不存在");
        }
    }
}