package top.doe.javase.executor;


import java.io.Serializable;

public class MyNoStaticUtil implements Serializable {

    MyMap myMap;

    public MyMap getCounter() {

        if (myMap == null) {
            synchronized ("ok") {
                if(myMap == null ) {
                    myMap = new MyMap();
                }
            }

        }
        return myMap;
    }

}
