package top.doe.hdfs.demos;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.ArrayList;
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
public class HdfsDemo2 {
    public static void main(String[] args) throws IOException {

        System.setProperty("HADOOP_USER_NAME","john");

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://doitedu01:8020");  // 指定HDFS集群的网络地址   namenode:8020
        conf.set("dfs.replication","3");  // 指定本客户端创建文件时指定的副本数
        conf.set("dfs.blocksize","32M");  // 指定本客户端创建的文件的block大小

        FileSystem fs = FileSystem.get(conf);

        // 读hdfs中的 /newyork.html文件的内容，统计文件中每个单词的出现次数，
        // 并把统计结果写入hdfs的 /output/res.txt


        // 用open方法打开一个文件的输入流
        FSDataInputStream din = fs.open(new Path("hdfs://doitedu01:8020/newyork.html"));
        //din.seek(100);


        BufferedReader br = new BufferedReader(new InputStreamReader(din));

        HashMap<String, Integer> countMap = new HashMap<>();

        String line;
        while( (line = br.readLine())!=null ){

            String[] words = line.split(" ");
            for (String word : words) {
                //countMap.put(word, countMap.getOrDefault(word, 0)+1);
                Integer oldValue = countMap.getOrDefault(word, 0);
                countMap.put(word, oldValue+1);
            }
        }
        br.close();
        din.close();

        // 将统计结果写入hdfs的  /output/res.txt
        FSDataOutputStream fdOut = fs.create(new Path("/output/res.txt"));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fdOut));

        for (Map.Entry<String, Integer> entry : countMap.entrySet()) {
            String word = entry.getKey();
            Integer count = entry.getValue();

            bw.write(word+" ==> " + count);
            bw.newLine();

        }

        bw.close();
        fdOut.close();
        fs.close();

    }

}
