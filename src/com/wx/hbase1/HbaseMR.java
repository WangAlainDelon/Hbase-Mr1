package com.wx.hbase1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseMR {

    /*
      这是一个运行MR的模板程序，主要是将一些文本输入hbase,然后读取hbase的信息，进行wordcount计算，最后再写入hbase
     */
    /**
     * 创建hbase配置
     */
    static Configuration config = null;
    static {
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum","zookeeper1");  //设置zookeeper地址
        config.set("hbase.zookeeper.property.clientPort", "2181"); //zookeeper端口
    }
    /**
     * 表信息
     */
    public static final String tableName = "word";//表名1 存放文本信息的表
    public static final String colf = "content";//列族
    public static final String col = "info";//列
    public static final String tableName2 = "stat";//表名2。存放计算后的信息的表
    /**
     * 初始化表结构，及其数据
     */
    public static void initTB() {
        HTable table=null;  //表类
        HBaseAdmin admin=null;  //管理类
        try {
            admin = new HBaseAdmin(config);//创建表管理
            /*删除表，事先判断如果这个表存在就删除*/
            if (admin.tableExists(tableName)||admin.tableExists(tableName2)) {
                System.out.println("table is already exists!");
                admin.disableTable(tableName); //删除的语法是要先置为不可用
                admin.deleteTable(tableName);
                admin.disableTable(tableName2);
                admin.deleteTable(tableName2);
            }
            /*创建表 创建表的描述类*/
            HTableDescriptor desc = new HTableDescriptor(tableName);
            //创建列族的描述类
            HColumnDescriptor family = new HColumnDescriptor(colf);
            desc.addFamily(family);
            admin.createTable(desc);
            //创建表二的描述类
            HTableDescriptor desc2 = new HTableDescriptor(tableName2);
            //创建表二的列族的描述类
            HColumnDescriptor family2 = new HColumnDescriptor(colf);
            desc2.addFamily(family2);
            admin.createTable(desc2);
            /*插入数据*/
            table = new HTable(config,tableName);
            table.setAutoFlush(false);
            table.setWriteBufferSize(500);
            List<Put> lp = new ArrayList<Put>();
            Put p1 = new Put(Bytes.toBytes("1"));
            p1.add(colf.getBytes(), col.getBytes(),	("The Apache Hadoop software library is a framework").getBytes());
            lp.add(p1);
            Put p2 = new Put(Bytes.toBytes("2"));
            p2.add(colf.getBytes(),col.getBytes(),("The common utilities that support the other Hadoop modules").getBytes());
            lp.add(p2);
            Put p3 = new Put(Bytes.toBytes("3"));
            p3.add(colf.getBytes(), col.getBytes(),("Hadoop by reading the documentation").getBytes());
            lp.add(p3);
            Put p4 = new Put(Bytes.toBytes("4"));
            p4.add(colf.getBytes(), col.getBytes(),("Hadoop from the release page").getBytes());
            lp.add(p4);
            Put p5 = new Put(Bytes.toBytes("5"));
            p5.add(colf.getBytes(), col.getBytes(),("Hadoop on the mailing list").getBytes());
            lp.add(p5);
            table.put(lp);
            table.flushCommits();
            lp.clear();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if(table!=null){
                    table.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * MyMapper 继承 TableMapper
     * TableMapper<Text,IntWritable>
     * Text:输出的key类型，
     * IntWritable：输出的value类型
     */
    public static class MyMapper extends TableMapper<Text, IntWritable> {
        private static IntWritable one = new IntWritable(1);
        private static Text word = new Text();
        @Override
        //输入的类型为：key：rowKey； value：一行数据的结果集Result
        protected void map(ImmutableBytesWritable key, Result value,
                           Context context) throws IOException, InterruptedException {
            //获取一行数据中的colf：col
            String words = Bytes.toString(value.getValue(Bytes.toBytes(colf), Bytes.toBytes(col)));// 表里面只有一个列族，所以我就直接获取每一行的值
            //按空格分割
            String itr[] = words.toString().split(" ");
            //循环输出word和1
            for (int i = 0; i < itr.length; i++) {
                word.set(itr[i]);
                context.write(word, one);
            }
        }
    }
    /**
     * MyReducer 继承 TableReducer
     * TableReducer<Text,IntWritable>
     * Text:输入的key类型，
     * IntWritable：输入的value类型，
     * ImmutableBytesWritable：输出类型，表示rowkey的类型
     */
    public static class MyReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //对mapper的数据求和
            int sum = 0;
            for (IntWritable val : values) {//叠加
                sum += val.get();
            }
            // 创建put，设置rowkey为单词
            Put put = new Put(Bytes.toBytes(key.toString()));
            // 封装数据
            put.add(Bytes.toBytes(colf), Bytes.toBytes(col),Bytes.toBytes(String.valueOf(sum)));
            //写到hbase,需要指定rowkey、put
            context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())),put);
        }
    }

    public static void main(String[] args) throws IOException,ClassNotFoundException, InterruptedException {
        //初始化表
        initTB();//初始化表
        //创建job
        Job job = Job.getInstance(config,"HbaseMR");//job
        job.setJarByClass(HbaseMR.class);//主类
        //创建scan
        Scan scan = new Scan();
        //可以指定查询某一列
        scan.addColumn(Bytes.toBytes(colf), Bytes.toBytes(col));
        //创建查询hbase的mapper，设置表名、scan、mapper类、mapper的输出key、mapper的输出value
        TableMapReduceUtil.initTableMapperJob(tableName, scan, MyMapper.class,Text.class, IntWritable.class, job);
        //创建写入hbase的reducer，指定表名、reducer类、job
        TableMapReduceUtil.initTableReducerJob(tableName2, MyReducer.class, job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
