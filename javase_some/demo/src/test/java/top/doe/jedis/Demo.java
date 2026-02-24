package top.doe.jedis;

import redis.clients.jedis.Jedis;

public class Demo {
    public static void main(String[] args) {


        Jedis jedis = new Jedis("doitedu03", 6379);
        jedis.hset("hs","k1","f1");

        jedis.close();


    }
}
