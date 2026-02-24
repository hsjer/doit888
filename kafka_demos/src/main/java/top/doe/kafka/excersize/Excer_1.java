package top.doe.kafka.excersize;

import java.util.Timer;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/10/2
 * @Desc: 学大数据，上多易教育
 *
 *    写一个程序，模拟一个数据源不断产生数据,并把生成的数据写入 kafka
 *    {"uid":1,"event_id":"add_cart","action_time":14334632589834,"page_url":"/aa/bbb"}
 *    {"uid":1,"event_id":"add_cart","action_time":14334632589834,"page_url":"/aa/bbb"}
 *    {"uid":1,"event_id":"add_cart","action_time":14334632589834,"page_url":"/aa/bbb"}
 *    {"uid":1,"event_id":"add_cart","action_time":14334632589834,"page_url":"/aa/bbb"}
 *    {"uid":1,"event_id":"add_cart","action_time":14334632589834,"page_url":"/aa/bbb"}
 *    {"uid":1,"event_id":"add_cart","action_time":14334632589834,"page_url":"/aa/bbb"}
 *
 *
 *    写一个程序：
 *       从 kafka 中消费上述的数据
 *       然后统计每 5秒中，add_cart事件的发生次数和发生人数
 *       并把结果，写入mysql
 *
 *
 **/
public class Excer_1 {
    public static void main(String[] args) {


    }




}
