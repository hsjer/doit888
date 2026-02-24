package top.doe.hdfs.demos;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/8/21
 * @Desc: 学大数据，上多易教育
 *   HDFS客户端读、写文件内容
 **/
public class HdfsDemo3 {
    public static void main(String[] args) throws IOException {

        System.setProperty("HADOOP_USER_NAME","john");

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://doitedu01:8020");  // 指定HDFS集群的网络地址   namenode:8020
        conf.set("dfs.replication","3");  // 指定本客户端创建文件时指定的副本数
        conf.set("dfs.blocksize","32M");  // 指定本客户端创建的文件的block大小

        FileSystem fs = FileSystem.get(conf);

        FSDataOutputStream fsDataOutputStream = fs.create(new Path("/warehouse/x.db/t_y/ssss.txt"));
        fsDataOutputStream.write("hello world".getBytes());

        fsDataOutputStream.close();
        fs.close();


    }

}
