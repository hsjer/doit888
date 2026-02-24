package top.doe.redis;

import redis.clients.jedis.Jedis;

public class TestConn {
    public static void main(String[] args) {


        Jedis jedis = new Jedis("192.168.10.7", 6379);
        String pong = jedis.ping();
        System.out.println(pong);


    }
}
