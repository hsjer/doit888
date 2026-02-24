package top.doe.javase.enclosure;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.ObjectInputStream;

public class App2 {
    public static void main(String[] args) throws Exception {

        ObjectInputStream oIn = new ObjectInputStream(new FileInputStream("./enclosure/f.obj"));

        Function o = (Function) oIn.readObject();

        System.out.println(o.call());

    }
}
