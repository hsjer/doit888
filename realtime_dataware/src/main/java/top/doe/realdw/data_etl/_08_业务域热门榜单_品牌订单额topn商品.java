package top.doe.realdw.data_etl;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import top.doe.realdw.utils.EnvUtil;

public class _08_业务域热门榜单_品牌订单额topn商品 {
    public static void main(String[] args) {

        StreamTableEnvironment tenv = EnvUtil.getTableEnv();

        TableConfig config = tenv.getConfig();
        // 开启窗口的提前触发器
        config.getConfiguration().setBoolean("table.exec.emit.early-fire.enabled",true);
        // 指定触发的间隔时长
        config.getConfiguration().setString("table.exec.emit.early-fire.delay","1000 ms");

        // 建表，映射 订单主表
        tenv.executeSql(
                "CREATE TABLE order_mysql (               " +
                        "      id BIGINT,                         " +   // 订单id
                        "      status INT,                        " +   // 订单状态
                        "      payment_time timestamp(3),         " +   // 订单支付时间
                        "      modify_time timestamp(3),          " +   // 数据更新时间
                        "      rt as modify_time        ,         " +
                        "      watermark for rt as rt - interval '0' second ,  " +
                        "      PRIMARY KEY (id) NOT ENFORCED      " +
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

        // 建表，映射  订单详情 表
        tenv.executeSql(
                        " create table oms_order_item(                "+
                        "      id       bigint                        "+
                        "      ,order_id bigint                       "+
                        "      ,product_id bigint                     "+
                        "      ,product_brand string                  "+
                        "      ,real_amount decimal(10,2)             "+
                        "      ,PRIMARY KEY (id) NOT ENFORCED         "+
                        " ) WITH (                                    "+
                        "      'connector' = 'mysql-cdc',             "+
                        "      'hostname' = 'doitedu01'   ,           "+
                        "      'port' = '3306'          ,             "+
                        "      'username' = 'root'      ,             "+
                        "      'password' = 'ABC123.abc123'  ,        "+
                        "      'database-name' = 'realtime_dw',       "+
                        "      'table-name' = 'oms_order_item'        "+
                        " )                                           "
        );


        // 关联
        tenv.executeSql(
                        "create temporary view joined as                                         " +
                        "select                                                                  "+
                        "   b.order_id                                                           "+
                        "   ,b.product_id                                                        "+
                        "   ,b.product_brand                                                     "+
                        "   ,b.real_amount                                                       "+
                        "   ,a.rt                                                                "+
                        " from order_mysql a                                                     "+
                        " join oms_order_item b                                                  "+
                        " on a.id = b.order_id                                                   "+
                        " where to_date(date_format(a.payment_time,'yyyy-MM-dd'))=current_date   "
        );


        // 聚合
        // tenv.executeSql(
        //                 "select\n" +
        //                 "    tumble_start(rt,interval '24' hour) as window_start,\n" +
        //                 "    tumble_end(rt,interval '24' hour) as window_end,\n" +
        //                 "    product_brand,\n" +
        //                 "    product_id,\n" +
        //                 "    sum(real_amount) as real_amount\n" +
        //                 "from joined\n" +
        //                 "group by tumble(rt,interval '24' hour),product_brand,product_id"
        // ).print();


        tenv.executeSql(
                         " SELECT                                                                                                          "+
                        "                                                                                                                 "+
                        " window_start                                                                                                    "+
                        " ,window_end                                                                                                     "+
                        " ,product_brand                                                                                                  "+
                        " ,product_id                                                                                                     "+
                        " ,real_amount                                                                                                    "+
                        " ,rn                                                                                                             "+
                        "                                                                                                                 "+
                        " FROM (                                                                                                          "+
                        "     select                                                                                                      "+
                        "     window_start                                                                                                "+
                        "     ,window_end                                                                                                 "+
                        "     ,product_brand                                                                                              "+
                        "     ,product_id                                                                                                 "+
                        "     ,real_amount                                                                                                "+
                        "     ,row_number() over(partition by window_start,window_end,product_brand order by real_amount desc ) as rn     "+
                        "     from (                                                                                                      "+
                        "         select                                                                                                  "+
                        "             tumble_start(rt,interval '24' hour) as window_start,                                                "+
                        "             tumble_end(rt,interval '24' hour) as window_end,                                                    "+
                        "             product_brand,                                                                                      "+
                        "             product_id,                                                                                         "+
                        "             sum(real_amount) as real_amount                                                                     "+
                        "         from joined                                                                                             "+
                        "         group by tumble(rt,interval '24' hour),product_brand,product_id                                         "+
                        "     ) o1                                                                                                        "+
                        " ) o2                                                                                                            "+
                        " WHERE rn<=2                                                                                                     "
        ).print();




    }
}
