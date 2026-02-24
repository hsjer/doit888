package top.doe.javase.executor;


import java.io.Serializable;
import java.util.HashMap;

public class Task implements Runnable, Serializable {

    int id;
    Function func;
    public Task(int id,Function func){
        this.id = id;
        this.func = func;
    }


    @Override
    public void run() {
       func.call();
    }
}
