package top.doe.redis;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import redis.clients.jedis.Jedis;

import java.math.BigDecimal;
import java.util.Map;

public class Demo2 {
    public static void main(String[] args) {

        // 创建客户端（或建立连接）
        Jedis jedis = new Jedis("doitedu01", 6379);

        // 调方法操作数据
        Product p1 = new Product(
                1,
                "燃魂",
                new BigDecimal("99.9"),
                new BigDecimal("99.8"),
                "http://pic_server/20240928/upload/000001.jpg", "<html><head></head></html>");

        Product p2 = new Product(
                2,
                "滴血",
                new BigDecimal("66.9"),
                new BigDecimal("66.8"),
                "http://pic_server/20240928/upload/000002.jpg", "<html><head></head></html>");



        // ---------------插入数据到一个hash结构 --------------------------------

        jedis.hset("products",(p1.getId()+""), JSON.toJSONString(p1));
        jedis.hset("products",(p2.getId()+""), JSON.toJSONString(p2));





        // ---------------从hash结构中取数据--------------------------------

        // 从hash中取一条数据
        String json1 = jedis.hget("products", "1");
        Product product1 = JSON.parseObject(json1, Product.class);
        System.out.println(product1);

        System.out.println("---------------------------------------");

        Map<String, String> products = jedis.hgetAll("products");
        for (Map.Entry<String, String> entry : products.entrySet()) {
            String pid = entry.getKey();
            String js = entry.getValue();
            Product p = JSON.parseObject(js, Product.class);
            System.out.println(p);

        }



        jedis.close();


    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Product {
        private int id;
        private String name;
        private BigDecimal price;
        private BigDecimal discountPrice;
        private String pic;
        private String desc;
    }

}
