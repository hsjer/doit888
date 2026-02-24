package top.doe.zookeeper.anli;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class Master {
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

        ServerSocket ss = new ServerSocket(9999);
        System.out.println(">>>> master绑定9999端口，开始工作 >>>>>");

        CountDownLatch countDownLatch = new CountDownLatch(1);

        // 向zookeeper注册
        ZooKeeper zkCli = new ZooKeeper("doitedu01:2181,doitedu02:2181,doitedu03:2181", 5000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                if(watchedEvent.getType() == Event.EventType.None) {
                    System.out.println(watchedEvent);
                    countDownLatch.countDown();
                }
            }
        });

        // 等待连接建立成功
        countDownLatch.await();

        String hostName = InetAddress.getLocalHost().getHostName();

        // 判断父znode是否存在
        Stat exists = zkCli.exists("/mydist", false);
        if(exists == null ){
            zkCli.create("/mydist",null, ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        }

        zkCli.create("/mydist/master",hostName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);

        // 创建业务线程开始工作
        new Thread(new ServiceRunnable(ss)).start();

        // 创建一个监听worker上下线的线程
        new Thread(new WorkerListenerRunnable(zkCli)).start();







    }


    public static class ServiceRunnable implements Runnable{
        ServerSocket ss;
        public ServiceRunnable(ServerSocket ss){
            this.ss = ss;
        }


        @Override
        public void run() {
            while(true){
                try {
                    Socket socket = ss.accept();
                    System.out.println("收到请求.................");


                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

            }
        }
    }


    public static class WorkerListenerRunnable implements Runnable{
        ZooKeeper zkCli;
        public WorkerListenerRunnable(ZooKeeper zkCli){
            this.zkCli = zkCli;
        }


        @Override
        public void run() {

            // /mydist/workers/doitedu01 8989
            // /mydist/workers/doitedu02 8989
            // /mydist/workers/doitedu03 8989

            try {
                List<String> children = zkCli.getChildren("/mydist/workers",new NodeChildrenWatcher(zkCli));
                System.out.println("此刻，在线的worker有：" + children);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        }



        public static class NodeChildrenWatcher implements Watcher{
            ZooKeeper zkCli;
            public NodeChildrenWatcher(ZooKeeper zkCli){
                this.zkCli = zkCli;
            }

            @Override
            public void process(WatchedEvent watchedEvent) {

                // 收到子节点变化事件
                if(watchedEvent.getType() == Event.EventType.NodeChildrenChanged){
                    try {

                        // 重新获取最新的子节点，并再次注册监听器
                        List<String> children = zkCli.getChildren("/mydist/workers", new NodeChildrenWatcher(zkCli));
                        System.out.println("worker节点发生变化，此刻的在线workers为：" + children);



                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

    }


}
