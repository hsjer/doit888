package top.doe.javase.executor;

import java.io.Serializable;

public class MyMap implements Serializable {
    private String key;
    private int value;

    public MyMap() {
    }

    public MyMap(String key, int value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }
}
