package top.doe.zookeeper.anli;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.concurrent.CountDownLatch;

public class Worker {

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

        //
        ServerSocket ss = new ServerSocket(9898);

        // 向zookeeper注册自己
        String hostName = InetAddress.getLocalHost().getHostName();

        CountDownLatch latch = new CountDownLatch(1);

        ZooKeeper cli = new ZooKeeper("doitedu01:2181,doitedu02:2181,doitedu03:2181", 5000, new Watcher() {

            @Override
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getType() == Event.EventType.None) {
                    latch.countDown();
                }
            }
        });

        latch.await();


        cli.create("/mydist/workers/"+hostName,"9898".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);


        while(true){
            Thread.sleep(Long.MAX_VALUE);
        }
    }
}
