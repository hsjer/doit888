package top.doe.javase.enclosure;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.*;

public class App {

    public static void main(String[] args) throws IOException {


        Person p = new Person("zhangsan");

        // 有名实现方式
        MyFunction myFunction = new MyFunction(p);
        String res = myFunction.call();
        System.out.println(res);


        // 这个匿名实现的内部类，引用了外部类的变量
        // 就会把这个变量捕获成自己的成员变量
        Function function = new Function() {
            @Override
            public String call() {
                return p.name;
            }
        };
        System.out.println(function.call());

        // 序列化这个匿名实现的对象
        ObjectOutputStream oOut = new ObjectOutputStream(new FileOutputStream("./enclosure/f.obj"));
        oOut.writeObject(function);


        // 用lambda表达式实现接口
        Function c = ()->p.name;
        c.call();



    }





    // 有名实现
    public static class MyFunction implements Function{
        Person p;
        public MyFunction(Person p ){
            this.p = p;
        }

        @Override
        public String call() {
            return p.name;
        }
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static  class Person implements Serializable {
        private String name;

    }

}
