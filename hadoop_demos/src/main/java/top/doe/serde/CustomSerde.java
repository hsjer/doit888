package top.doe.serde;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.Test;

import java.io.*;

public class CustomSerde {

    @Test
    public  void testSer() throws IOException {
        Student st1 = new Student("张三", 18, 1000.0f, true, 80.0);
        // 序列化,并写入文件
        FileOutputStream fos = new FileOutputStream("st.obj");
        DataOutputStream dout = new DataOutputStream(fos);

        dout.writeUTF(st1.getName()); // dout会先写2个字节表达字符串的长度信息，然后把字符串按utf编码生成字节数组
        dout.writeFloat(st1.getSalary());
        dout.writeBoolean(st1.isMarried());
        dout.writeInt(st1.getAge());
        dout.writeDouble(st1.getWeight());
        dout.close();
    }


    @Test
    public  void testSer2() throws IOException {
        Student st1 = new Student("张三", 18, 1000.0f, true, 80.0);
        // 序列化,并写入文件
        FileOutputStream fos = new FileOutputStream("d:/st2.obj");
        ObjectOutputStream oo = new ObjectOutputStream(fos);
        oo.writeObject(st1);
        oo.writeInt(4);


        oo.close();
    }



    @Test

    public  void testDe() throws IOException {
        FileInputStream in = new FileInputStream("st.obj");
        DataInputStream din = new DataInputStream(in);

        String name = din.readUTF();  // din会先读2字节得到utf字符串的长度，再按长度读字节，解码成一个字符串返回
        float salary = din.readFloat();
        boolean married = din.readBoolean();
        int age = din.readInt();
        double weight = din.readDouble();

        din.close();

        Student st1 = new Student(name, age, salary, married, weight);


        System.out.println(st1);


    }



    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Student implements Serializable{
        private String name;
        private int age;
        private float salary;
        private boolean married;
        private double weight;

    }
}
