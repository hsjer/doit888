package top.doe.yarn;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

public class ResourceManager {
    public static void main(String[] args) throws IOException {

        // 与 集群内NodeManager通信用的
        ServerSocket ss1 = new ServerSocket(16010);

        // 与 分布式运算程序的作业客户端通信
        ServerSocket ss2 = new ServerSocket(16020);

        ArrayList<NodeManagerInfo> nmList = new ArrayList<>();

        // 接收NM注册
        new Thread(new RegisterNodeManagerRunnable(ss1,nmList)).start();

        // 接收作业提交请求
        new Thread(new SubmitApplicationRunnable(ss2,nmList)).start();



    }

}
