package top.doe.javase.executor;

import org.apache.commons.lang3.RandomUtils;

import java.io.*;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SparkExecutor {

    public static void main(String[] args) throws IOException, ClassNotFoundException {

        ExecutorService pool = Executors.newFixedThreadPool(5);

        // 测试静态工具
        /* for(int i=0;i<20;i++) {
            Task task = new Task(i,()->{
                HashMap<String, Integer> counter = MyStaticCounterUtil.getCounter();
                System.out.println("我的counter: " + counter.hashCode());
            });
            pool.submit(task);
        }*/

        // 测试非静态工具,测试是失效的
        // 因为每个task对象中闭包引用的上外面的同一个util对象，
        // 所以，task中获取counter的时候，只会创建1次
        MyNoStaticUtil myNoStaticUtil = new MyNoStaticUtil();

        for(int i=0;i<20;i++) {
            Task task = new Task(i,()->{
                MyMap counter = myNoStaticUtil.getCounter();
                counter.setKey(System.currentTimeMillis()+"");
                counter.setValue((int)System.currentTimeMillis());
                System.out.println("我的counter: " + counter);
            });
            pool.submit(task);
        }

        //
        /* for (int i = 0; i < 5; i++) {
            Task task = new Task(
                    i,
                    () -> {

                        System.out.println("准备获取Counter.......");

                        MyMap counter = myNoStaticUtil.getCounter();
                        System.out.println("成功拿到Counter.......");


                        counter.setKey(System.currentTimeMillis()+"");
                        counter.setValue((int)System.currentTimeMillis());
                        System.out.println("放数据到counter.......");


                        System.out.println("我的counter: " + counter);
                    });

            // task要被序列化
            ByteArrayOutputStream baout = new ByteArrayOutputStream();
            ObjectOutputStream oOut = new ObjectOutputStream(baout);
            oOut.writeObject(task);

            byte[] bytes = baout.toByteArray();
            oOut.close();
            baout.close();


            // executor收到后会反序列化
            ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
            ObjectInputStream oIn = new ObjectInputStream(bin);
            Task taskDes = (Task) oIn.readObject();


            // 放入线程池执行
            pool.submit(taskDes);
        }*/

    }

}
