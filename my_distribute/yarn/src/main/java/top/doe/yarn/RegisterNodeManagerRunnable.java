package top.doe.yarn;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

public class RegisterNodeManagerRunnable implements Runnable{

    ServerSocket ss;
    ArrayList<NodeManagerInfo> nmList;

    public RegisterNodeManagerRunnable(ServerSocket ss, ArrayList<NodeManagerInfo> nmList) {
        this.ss = ss;
        this.nmList = nmList;
    }

    @Override
    public void run() {

        while(true){

            try {
                Socket sc = ss.accept();

                InputStream in = sc.getInputStream();
                OutputStream out = sc.getOutputStream();

                ObjectOutputStream oOut = new ObjectOutputStream(out);
                ObjectInputStream oIn = new ObjectInputStream(in);

                NodeManagerInfo nmInfo = (NodeManagerInfo) oIn.readObject();


                nmList.add(nmInfo);
                System.out.println("==> 收到一个NM的注册信息: " + nmInfo);

                oOut.writeObject(new RegisterResponse("success"));

                oOut.close();
                oIn.close();
                sc.close();

            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        }
    }
}
