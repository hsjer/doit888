package top.doe.realdw.data_etl;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import top.doe.realdw.utils.EnvUtil;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/11/29
 * @Desc: 学大数据，上多易教育
 *       订单总数、总额 、应付总额 （当日新订单，累计到此刻）
 *       待支付订单数、订单额  （当日新订单，累计到此刻）
 *       已支付订单数、订单额  （当日支付，累计到此刻）
 *       已发货订单数、订单额  （当日发货，累计到此刻）
 *       已完成订单数、订单额  （当日完成，累计到此刻）
 *
 *       要求每秒更新
 **/
public class _07_业务域实时看板_订单日清日结看板 {
    public static void main(String[] args) {
        StreamTableEnvironment tenv = EnvUtil.getTableEnv();

        TableConfig config = tenv.getConfig();
        // 开启窗口的提前触发器
        config.getConfiguration().setBoolean("table.exec.emit.early-fire.enabled",true);
        // 指定触发的间隔时长
        config.getConfiguration().setString("table.exec.emit.early-fire.delay","1000 ms");


        // 建表，映射业务库所连接的mysql的订单表的binlog （理论应该是映射kafka中的）
        tenv.executeSql(
                "CREATE TABLE order_mysql (               " +
                        "      id BIGINT,                         " +  // 订单id
                        "      status INT,                        " +  // 订单状态
                        "      total_amount decimal(10,2),        " +  // 订单总额
                        "      pay_amount decimal(10,2),          " +  // 应付（实付）总额
                        "      create_time timestamp(3),          " +  // 订单创建时间
                        "      payment_time timestamp(3),         " +  // 支付时间
                        "      delivery_time timestamp(3),        " +  // 发货时间
                        "      confirm_time timestamp(3),         " +   // 确认时间
                        "      modify_time timestamp(3),          " +   // 数据更新时间
                        "      rt as modify_time        ,         " +
                        "      watermark for rt as rt - interval '0' second ,  " +
                        "     PRIMARY KEY (id) NOT ENFORCED       " +
                        "     ) WITH (                            " +
                        "     'connector' = 'mysql-cdc',          " +
                        "     'hostname' = 'doitedu01'   ,        " +
                        "     'port' = '3306'          ,          " +
                        "     'username' = 'root'      ,          " +
                        "     'password' = 'ABC123.abc123'  ,     " +
                        "     'database-name' = 'realtime_dw',    " +
                        "     'table-name' = 'oms_order'          " +
                        ")"
        );


        // 建表，映射  大屏看板连所接的mysql中的目标表

        tenv.executeSql(
                        " create table mysql_sink (                          "+
                        " window_start             timestamp(3)              "+
                        " ,window_end               timestamp(3)             "+
                        " ,od_new_cnt               bigint                   "+
                        " ,od_new_amt               decimal(10,2)            "+
                        " ,od_new_pay_amt           decimal(10,2)            "+
                        " ,od_new_topay_cnt         bigint                   "+
                        " ,od_new_topay_amt         decimal(10,2)            "+
                        " ,od_pay_cnt               bigint                   "+
                        " ,od_pay_amt               decimal(10,2)            "+
                        " ,od_delivery_cnt          bigint                   "+
                        " ,od_delivery_amt          decimal(10,2)            "+
                        " ,od_confirm_cnt           bigint                   "+
                        " ,od_confirm_amt           decimal(10,2)            "+
                        " ,PRIMARY KEY(window_start,window_end) NOT ENFORCED  "+
                        " ) WITH (                                           "+
                        "    'connector' = 'jdbc',                           "+
                        "    'url' = 'jdbc:mysql://doitedu01:3306/dw_50',    "+
                        "    'table-name' = 'dashboard_02_metric',           "+
                        "    'driver' = 'com.mysql.cj.jdbc.Driver',          "+
                        "    'username' = 'root',                            "+
                        "    'password' = 'ABC123.abc123'                    "+
                        " );                                                 "
        );



        // 计算逻辑sql（累计窗口，长度为24小时，触发周期1秒）
        tenv.executeSql(
                         " INSERT INTO mysql_sink                                                                                     "+
                         " WITH tmp AS (                                                                                               "+
                        " SELECT                                                                                                      "+
                        " id                                                                                                          "+
                        " ,status                                                                                                     "+
                        " ,total_amount                                                                                               "+
                        " ,pay_amount                                                                                                 "+
                        " ,to_date(date_format(create_time,'yyyy-MM-dd')  )  as create_date                                           "+
                        " ,to_date(date_format(payment_time,'yyyy-MM-dd') )  as payment_date                                          "+
                        " ,to_date(date_format(delivery_time,'yyyy-MM-dd'))  as delivery_date                                         "+
                        " ,to_date(date_format(confirm_time,'yyyy-MM-dd') )  as confirm_date                                          "+
                        " ,rt                                                                                                         "+
                        " from order_mysql                                                                                            "+
                        " WHERE  to_date(date_format(create_time,'yyyy-MM-dd')  ) = CURRENT_DATE                                      "+
                        "     OR to_date(date_format(payment_time,'yyyy-MM-dd') ) = CURRENT_DATE                                      "+
                        "     OR to_date(date_format(delivery_time,'yyyy-MM-dd')) = CURRENT_DATE                                      "+
                        "     OR to_date(date_format(confirm_time,'yyyy-MM-dd') ) = CURRENT_DATE                                      "+
                        " )                                                                                                           "+
                        "                                                                                                             "+
                        " SELECT                                                                                                        "+
                        "  tumble_start(rt,interval '24' hour)  as window_start                                   "+
                        " ,tumble_end(rt,interval '24' hour)  as window_end                                       "+
                        " ,count(id) filter(where create_date = CURRENT_DATE ) as od_new_cnt                                            "+
                        " ,sum(total_amount) filter(where create_date = CURRENT_DATE ) as od_new_amt                                    "+
                        " ,sum(pay_amount) filter(where create_date = CURRENT_DATE ) as od_new_pay_amt                                  "+
                        " ,count(id) filter(where create_date = CURRENT_DATE AND payment_date is null ) as od_new_topay_cnt             "+
                        " ,sum(total_amount) filter(where create_date = CURRENT_DATE AND payment_date is null ) as od_new_topay_amt     "+
                        " ,count(id) filter(where payment_date = CURRENT_DATE ) as od_pay_cnt                                           "+
                        " ,sum(total_amount) filter(where payment_date = CURRENT_DATE ) as od_pay_amt                                   "+
                        " ,count(id) filter(where delivery_date = CURRENT_DATE ) as od_delivery_cnt                                     "+
                        " ,sum(total_amount) filter(where delivery_date = CURRENT_DATE ) as od_delivery_amt                             "+
                        " ,count(id) filter(where confirm_date = CURRENT_DATE ) as od_confirm_cnt                                       "+
                        " ,sum(total_amount) filter(where confirm_date = CURRENT_DATE ) as od_confirm_amt                               "+
                        "                                                                                                               "+
                        " FROM tmp                                                                                                      "+
                        " group by tumble(rt,interval '24' hour)                                                                        "
        ).print();





    }
}
