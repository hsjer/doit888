package top.doe.yarn;

import top.doe.common.yarn.ApplicationRequest;
import top.doe.common.yarn.ApplicationResponse;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

/**
 * 接收作业提交的 Runnable
 */
public class SubmitApplicationRunnable implements Runnable {

    ServerSocket ss;
    ArrayList<NodeManagerInfo> nmList;
    int count = 1;

    public SubmitApplicationRunnable(ServerSocket ss, ArrayList<NodeManagerInfo> nmList) {
        this.ss = ss;
        this.nmList = nmList;
    }

    @Override
    public void run() {

        while (true) {
            try {
                // 接收请求
                Socket sc = ss.accept();
                InputStream in = sc.getInputStream();
                OutputStream out = sc.getOutputStream();

                ObjectOutputStream oOut = new ObjectOutputStream(out);
                ObjectInputStream oIn = new ObjectInputStream(in);

                // 一、获得请求者发送过来的请求信息对象
                ApplicationRequest request = (ApplicationRequest) oIn.readObject();

                // 二、请求者所需要的容器数
                int containerNumb = request.getContainerNumb();

                // 三、从注册列表中挑选node manager
                // 如果用户请求的容器数  超过了  集群的 "node manager数*2",就让submit失败
                if (containerNumb > 2 * nmList.size()) {
                    oOut.writeObject(new ApplicationResponse("fail", null, null));
                    oOut.flush();
                } else {
                    // 从resource manager 所持有的  已注册 node manager列表中，挑选node manager
                    ArrayList<String> list = new ArrayList<>();
                    for (int i = 0; i < containerNumb; i++) {

                        int mod =  i % nmList.size();
                        NodeManagerInfo nodeManagerInfo = nmList.get(mod);
                        // doitedu02:16030
                        String hostAndPort = nodeManagerInfo.getHost() + ":" + nodeManagerInfo.getPort();

                        if (!list.contains(hostAndPort)) {
                            list.add(hostAndPort);
                        }
                    }

                    // 四、将挑选出来的node manager的 host:port列表，以及一个 application-id，返回给 请求者
                    oOut.writeObject(new ApplicationResponse("success", list, "application-" + count++));
                    oOut.flush();
                }

                oOut.close();
                oIn.close();
                sc.close();

            } catch (Exception e) {
                throw new RuntimeException(e);
            }


        }
    }
}
