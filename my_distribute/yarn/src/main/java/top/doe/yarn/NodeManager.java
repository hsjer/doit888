package top.doe.yarn;

import top.doe.common.yarn.AppLaunchContext;
import top.doe.common.yarn.AppLaunchResp;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

public class NodeManager {

    public static void main(String[] args) throws Exception {

        boolean register = register(args[0], Integer.parseInt(args[1]));
        if (!register) {
            return;  // 注册失败，直接退出
        }


        // 2. 绑定一个端口，用来接收客户端的启动程序的请求
        ServerSocket ss = new ServerSocket(16030);
        System.out.println("NM监听端口，等待客户端请求 ===> 16030");
        while (true) {
            Socket sc2 = ss.accept();
            System.out.println("==> NM接收到客户端的请求");
            InputStream in2 = sc2.getInputStream();
            OutputStream out2 = sc2.getOutputStream();

            ObjectOutputStream oo = new ObjectOutputStream(out2);
            ObjectInputStream oi = new ObjectInputStream(in2);

            //  处理客户端启动程序的请求
            //  启动用户（分布式运算作业）的程序所需的信息 AppLaunchContext：
            // 1. jar包
            // 2. 启动类（入口主类）
            // 3. 启动参数
            AppLaunchContext appLaunchContext = (AppLaunchContext) oi.readObject();
            byte[] jarBytes = appLaunchContext.getJarBytes();
            String mainClass = appLaunchContext.getMainClass();
            String paramJson = appLaunchContext.getParamJson();

            // 为用户进程创建工作目录
            String applicationId = appLaunchContext.getApplicationId();
            String containerId = appLaunchContext.getTaskId();
            String workDir = "/tmp/nm_local/user/" + applicationId + "/" + containerId;
            new File(workDir).mkdirs();


            // 将jar包写入即将启动的作业进程的工作目录
            //  路径：  /tmp/nm_local/user/application_001/task-001/job.jar
            FileOutputStream fo = new FileOutputStream(new File(workDir + "/job.jar"));
            fo.write(jarBytes);
            fo.close();


            // 创建资源容器
            /**
             * mkdir /sys/fs/cgroup/cpu/hitao
             * mkdir /sys/fs/cgroup/memory/hitao
             *
             * echo 50000 > /sys/fs/cgroup/cpu/hitao/cpu.cfs_quota_us
             * echo 268435456 > /sys/fs/cgroup/memory/hitao/memory.limit_in_bytes
             */
            String containerCgPath = applicationId + "_" + containerId;
            Runtime.getRuntime().exec("mkdir /sys/fs/cgroup/cpu/"+containerCgPath);
            Runtime.getRuntime().exec("mkdir /sys/fs/cgroup/memory/"+containerCgPath);

            // cpu占 50%
            Runtime.getRuntime().exec(new String[]{"sh","-c","echo 50000 > /sys/fs/cgroup/cpu/"+containerCgPath+"/cpu.cfs_quota_us"});
            // 内存用256M
            Runtime.getRuntime().exec(new String[]{"sh","-c","echo 268435456 > /sys/fs/cgroup/memory/"+containerCgPath+"/memory.limit_in_bytes"});

            // 启动用户作业程序
            // cgexec -g cpu,memory:hitao java -cp cg.jar top.doe.CgTask
            ProcessBuilder processBuilder = new ProcessBuilder("cgexec","-g","cpu,memory:"+containerCgPath,"java","-cp","job.jar",mainClass,paramJson);
            System.out.println("即将执行命令：=> " + "java"+" -cp"+" job.jar "+mainClass+" "+paramJson);
            System.out.println("-----------------------------------------------------");


            // 设置作业进程的工作目录为上面创建的工作目录（也是我们生成的job.jar所在的目录）
            processBuilder.directory(new File(workDir));
            Process process = processBuilder.start();


            oo.writeObject(new AppLaunchResp("success"));

            oo.flush();
            oo.close();
            oi.close();
            in2.close();
            out2.close();
            sc2.close();



            //process.waitFor();

        }
    }

    private static boolean register(String resourceManagerHost, int resourceManagerPort) throws
            IOException, ClassNotFoundException {
        // 1. 向resourceManager注册
        String rmHost = resourceManagerHost;
        int rmPort = resourceManagerPort;

        // 连接resource manager的 注册端口
        Socket sc = new Socket(rmHost, rmPort);
        InputStream in = sc.getInputStream();
        OutputStream out = sc.getOutputStream();

        // 坑： 一定要先构建  ObjectOutputStream
        ObjectOutputStream oOut = new ObjectOutputStream(out);
        ObjectInputStream oIn = new ObjectInputStream(in);

        // 获取自己的hostname
        String hostName = InetAddress.getLocalHost().getHostName();

        // 封装自己的注册信息，并发送给resource Manager
        oOut.writeObject(new NodeManagerInfo(hostName, 16030));

        // 获取resourceManager的注册响应
        RegisterResponse registerResponse = (RegisterResponse) oIn.readObject();
        String status = registerResponse.getStatus();
        if (!status.equals("success")) {
            System.out.println("==> NM注册失败");
            return false;
        } else {
            System.out.println("==> NM " + hostName + " 注册成功");
        }
        return true;
    }


}
