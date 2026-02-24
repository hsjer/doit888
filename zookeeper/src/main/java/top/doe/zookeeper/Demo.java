package top.doe.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class Demo {
    public static void main(String[] args) throws Exception {


        // 创建zookeeper的客户端对象
        ZooKeeper zkCli = new ZooKeeper(
                "doitedu01:2181,doitedu02:2181,doitedu03:2181",
                5000,
                new Watcher() {
                    @Override
                    public void process(WatchedEvent watchedEvent) {
                        System.out.println("收到一个通知：" + watchedEvent);

                    }
                });

        // 获取一个znode的数据
        byte[] data = zkCli.getData("/aa/x", false, null);

        String str = new String(data);
        System.out.println(str);


        zkCli.close();
    }
}
