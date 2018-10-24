package com.wx.hbase1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Table;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class Hbase1 {
    /*
    连接hbase集群使用api操作数据
     */
    //配置ss
    static Configuration configuration=null;
    private Connection connection=null;
    private Table table=null;
    @Before
    public void init () throws Exception
    {
        configuration = HBaseConfiguration.create();//分布式集群一定要先来配置
        //要通过zoookeeper来操作
        configuration.set("hbase.zookeeper.quorum","zookeeper1");//zookeeper地址
        configuration.set("hbase.zookeeper.property.clientPort","2181");//zookeeper端口
        connection = ConnectionFactory.createConnection(configuration);
        table=connection.getTable(TableName.valueOf("user"));
    }

    //创建表
    @Test
    public void createTable() throws Exception
    {
        //创建表的管理类
        HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
        //创建表的描述类
        TableName tableName = TableName.valueOf("test1");
        HTableDescriptor descriptor = new HTableDescriptor(tableName);
        //创建列族的描述类
        HColumnDescriptor family1 = new HColumnDescriptor("info");// 列族
        // 将列族添加到表中
        descriptor.addFamily(family1);
        HColumnDescriptor family2 = new HColumnDescriptor("info2"); // 列族
        // 将列族添加到表中
        descriptor.addFamily(family2);
        // 创建表
        hBaseAdmin.createTable(descriptor); // 创建表

    }
    @After
    public void close() throws Exception {
        table.close();
        connection.close();
    }
}
