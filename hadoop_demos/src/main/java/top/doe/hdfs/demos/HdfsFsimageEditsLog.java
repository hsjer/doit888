package top.doe.hdfs.demos;

import java.io.*;
import java.util.HashMap;

public class HdfsFsimageEditsLog {

//    public static void main(String[] args) throws Exception {
//        HashMap<String, String> map = new HashMap<>();
//        ObjectOutputStream oo = new ObjectOutputStream(new FileOutputStream("./edits_log/fsimage_0000000000"));
//        oo.writeObject(map);
//        oo.close();
//    }



    public static void main(String[] args) throws Exception {

        // 加载之前的元数据镜像文件，恢复出内存元数据对象
        ObjectInputStream oi = new ObjectInputStream(new FileInputStream("./edits_log/fsimage_0000000000"));
        HashMap<String, String> map = (HashMap<String, String>) oi.readObject();

        BufferedReader br = new BufferedReader(new FileReader("./edits_log/edits_000000000000"));
        String line;
        while((line = br.readLine())!=null){
            String[] split = line.split(",");
            if(split[0].equals("C")){
                String path = split[1];
                String fileName = split[2];

                map.put(path,fileName);
            }else if(split[0].equals("D")){
                String path = split[1];

                map.remove(path);
            }
        }



        BufferedWriter bw = new BufferedWriter(new FileWriter("./edits_log/edits_000000000000"));

        // 客户端请求创建新文件
        map.put("/ccc","c.txt");
        bw.write("C,/ccc,c.txt");
        bw.newLine();


        // 客户端请求创建新文件
        map.put("/aaa","a.txt");
        bw.write("C,/aaa,a.txt");
        bw.newLine();


        // 客户端请求创建新文件
        map.put("/aaa/bbb","b.txt");
        bw.write("C,/aaa/bbb,b.txt");
        bw.newLine();



        // 客户端请求删除  /aaa/a.txt
        map.remove("/aaa");
        bw.write("D,/aaa");
        bw.newLine();

        bw.flush();
        bw.close();





    }

}
