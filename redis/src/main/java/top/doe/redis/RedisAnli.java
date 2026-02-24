package top.doe.redis;


import org.apache.commons.lang3.RandomStringUtils;
import redis.clients.jedis.Jedis;

import java.util.Random;
import java.util.Scanner;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/9/28
 * @Desc: 学大数据，上多易教育
 * 用控制台模拟一个web后端程序：
 * 给用户生成一个手机验证码（4位数字），放redis中
 * 然后等待用户填写验证码（最长等待时间1分钟，如果超过一分钟则该验证码失效）
 * 如果用户填写的验证码正确，则打印正确
 **/
public class RedisAnli {

    public static void main(String[] args) {

        Random random = new Random();
        Jedis jedis = new Jedis("doitedu01", 6379);

        System.out.print("请输入登录手机号:");
        Scanner scanner = new Scanner(System.in);
        String phone = scanner.nextLine();


        // 生成验证码
        String code = RandomStringUtils.randomNumeric(4);

        // 发送给用户的手机
        System.out.println("假设这是发到用户手机的验证码： " + code);
        // 存到redis
        jedis.setex(phone, 10, code);


        for (int i = 0; i < 3; i++) {
            System.out.print(i == 0 ? "请输入验证码: " : "请重新输入正确的验证码: ");
            String userCode = scanner.nextLine();

            // 从redis中获取目标用户的验证码
            String redisCode = jedis.get(phone);

            if (verify(redisCode, userCode)) {
                System.out.println("验证通过,愉快滴玩耍吧");
                break;
            }
        }

    }

    public static boolean verify(String redisCode, String userCode) {
        if (redisCode == null || !redisCode.equals(userCode)) {
            return false;
        }
        return true;
    }
}
