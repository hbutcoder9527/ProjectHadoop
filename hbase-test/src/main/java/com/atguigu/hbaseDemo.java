package com.atguigu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class hbaseDemo {

    private static Connection connection = null;
    private static Admin admin = null;

    static {

        try {
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void close() {
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static boolean isExsit(String tableName) throws IOException {

        boolean b = admin.tableExists(TableName.valueOf(tableName));

        return b;
    }

    public static void creatable(String tableName, String... cfs) throws IOException {
        if (cfs.length <= 0) {
            System.out.println("列族信息输入错误！");
            return;
        }

        if (isExsit(tableName)) {
            System.out.println(tableName + "表已经存在!!!");
            return;
        }

        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        for (String cf : cfs) {

            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);

            tableDescriptor.addFamily(hColumnDescriptor);
        }

        admin.createTable(tableDescriptor);
    }

    public static void putData(String tableName, String rowkey, String cf, String na, String value) throws IOException {

        TableName tableName1 = TableName.valueOf(tableName);

        Table table = connection.getTable(tableName1);

        if (tableName == null) {

            System.out.println("查无此表，请重新输入");
            return;
        }

        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(na),Bytes.toBytes(value));

        table.put(put);

        table.close();

    }

    public static void main(String[] args) throws IOException {
//        System.out.println(isExsit("try"));
//
//        creatable("try", "info1", "info2");
//
//        System.out.println(isExsit("try"));
        putData("try","1001","info1","name","不与傻逼论长短");
        close();
    }
}
