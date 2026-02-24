package top.doe.hdfs.demos;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;
import java.util.Arrays;

public class HdfsDemo1 {
    public static void main(String[] args) throws IOException {

        // 创建配置对象 : 构造过程中会自动从classpath中加载：
        // hdfs-site.xml,core-site.xm,mapred-site.xml,yarn-site.xml
        Configuration conf = new Configuration();


        conf.set("fs.defaultFS", "hdfs://xxx/");
        conf.set("fs.defaultFS", "hdfs://doitedu01:8020/");  // 指定HDFS集群的网络地址   namenode:8020
        conf.set("dfs.replication","2");  // 指定本客户端创建文件时指定的副本数
        conf.set("dfs.blocksize","32M");  // 指定本客户端创建的文件的block大小

        // 创建hdfs客户端对象
        FileSystem fs = FileSystem.get(conf);

        // 调用客户端 创建文件夹
        // boolean bl = fs.mkdirs(new Path("/doit50/day05"));

        // 调用客户端 从本地拷贝一个文件到 HDFS
        // fs.copyFromLocalFile(new Path("xxx/log4j.properties"),new Path("/doit50/day05/"));


        RemoteIterator<LocatedFileStatus> iter = fs.listFiles(new Path("/"), true);
        while(iter.hasNext()){
            // 迭代返回的结果对象中，封装了文件的元信息
            LocatedFileStatus status = iter.next();

            // 文件的总长度
            long fileLen = status.getLen();
            System.out.println("文件长度： " + fileLen);

            // 最后访问时间
            long accessTime = status.getAccessTime();
            System.out.println("文件最后访问时间： " + accessTime);

            // 副本数
            short replication = status.getReplication();
            System.out.println("文件的副本数： " + replication);

            // 权限信息
            FsPermission permission = status.getPermission();
            System.out.println("文件的权限： " + permission);

            // 所在路径
            Path path = status.getPath();
            System.out.println("文件的路径： " + path);

            // 最后修改时间
            long modificationTime = status.getModificationTime();
            System.out.println("文件最后修改时间： " + modificationTime);

            // 文件的所有block块的信息
            BlockLocation[] blockLocations = status.getBlockLocations();

            System.out.println("-----------块信息 start --------------------");
            for (BlockLocation blockLocation : blockLocations) {

                // 当前遍历到块所在的 datanode主机名
                String[] hosts = blockLocation.getHosts();
                System.out.println(Arrays.asList(hosts));

                // 块的长度
                long blockLength = blockLocation.getLength();
                System.out.println("块长度: " + blockLength);

                // block所在的datanode的ip:port
                String[] names = blockLocation.getNames();
                System.out.println(Arrays.asList(names));

                // 当前块在文件中的起始位置
                long offset = blockLocation.getOffset();
                System.out.println("块在文件中的偏移量: " + offset);
            }
            System.out.println("-----------块信息 end--------------------");


        }


        fs.close();
    }
}
