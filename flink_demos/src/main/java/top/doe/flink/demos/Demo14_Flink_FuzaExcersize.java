package top.doe.flink.demos;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.json.DecimalFormat;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.roaringbitmap.RoaringBitmap;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;

public class Demo14_Flink_FuzaExcersize {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");
        env.disableOperatorChaining(); // 全局禁用算子链：每个算子都将成为一个独立的task

        // 创建source
        HashMap<String, Object> configs = new HashMap<>();
        configs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name());

        MySqlSource<String> source = MySqlSource.<String>builder()
                .username("root")
                .password("ABC123.abc123")
                .hostname("doitedu01")
                .port(3306)
                .databaseList("doit50")
                .tableList("doit50.oms_order")
                .deserializer(new JsonDebeziumDeserializationSchema(false, configs))
                .build();

        //DataStreamSource<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "order_cdc").setParallelism(2);
        SingleOutputStreamOperator<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "order_cdc").setParallelism(2).disableChaining();

        SingleOutputStreamOperator<CdcBean> beanStream = stream.shuffle().map(json -> JSON.parseObject(json, CdcBean.class)).setParallelism(2);

        // 预处理
        SingleOutputStreamOperator<DataBean> dataBeanStream = beanStream.process(new ProcessFunction<CdcBean, DataBean>() {

            @Override
            public void processElement(CdcBean cdcBean, ProcessFunction<CdcBean, DataBean>.Context ctx, Collector<DataBean> out) throws Exception {

                String op = cdcBean.op;
                OrderInfo before = cdcBean.before;
                OrderInfo after = cdcBean.after;

                LocalDate today = LocalDate.now();

                switch (op) {
                    case "r":
                    case "c":
                        LocalDate createTime = after.create_time.toLocalDate();
                        LocalDate payDate = after.payment_time == null ? null : after.payment_time.toLocalDate();
                        LocalDate deliverDate = after.delivery_time != null ? after.delivery_time.toLocalDate() : null;
                        LocalDate confirmDate = after.confirm_time != null ? after.confirm_time.toLocalDate() : null;

                        // 维度1：今日新订单
                        if (createTime.equals(today)) {
                            out.collect(new DataBean(1, after.id, after.total_amount, after.pay_amount, true));
                        }

                        // 维度2：今日新待支付订单
                        if (createTime.equals(today) && after.status == 0) {
                            out.collect(new DataBean(2, after.id, after.total_amount, after.pay_amount, true));
                        }

                        // 维度3：今日支付
                        if (payDate != null && payDate.equals(today)) {
                            out.collect(new DataBean(3, after.id, after.total_amount, after.pay_amount, true));
                        }

                        // 维度4: 今日发货
                        if (deliverDate != null && deliverDate.equals(today)) {
                            out.collect(new DataBean(4, after.id, after.total_amount, after.pay_amount, true));
                        }

                        // 维度5： 今日确认
                        if (confirmDate != null && confirmDate.equals(today)) {
                            out.collect(new DataBean(5, after.id, after.total_amount, after.pay_amount, true));
                        }
                        break;

                    case "u":
                        int status_1 = before.status;
                        int status_2 = after.status;

                        BigDecimal totalAmount_1 = before.total_amount;
                        BigDecimal totalAmount_2 = after.total_amount;

                        BigDecimal payAmount_1 = before.pay_amount;
                        BigDecimal payAmount_2 = after.pay_amount;

                        int oid = after.id;
                        LocalDate cDate = after.create_time.toLocalDate();
                        LocalDate pDate = after.payment_time != null ? after.payment_time.toLocalDate() : null;
                        LocalDate dDate = after.delivery_time != null ? after.delivery_time.toLocalDate() : null;
                        LocalDate cfDate = after.confirm_time != null ? after.confirm_time.toLocalDate() : null;

                        /**
                         * 有一个假设：
                         * 订单的状态变化必定是符合如下规则： 0->待付款；1->待发货；2->已发货；3->已完成；
                         * 订单状态从 待付->已付 变化时，才有可能产生金额的修改变化
                         */

                        // 0 待支付 -> 1 已付
                        if (!cDate.equals(today) && pDate != null && pDate.equals(today) && status_1 == 0 && status_2 == 1) {
                            out.collect(new DataBean(3, oid, totalAmount_2, payAmount_2, true));
                        } else if (cDate.equals(today) && pDate != null && pDate.equals(today) && status_1 == 0 && status_2 == 1) {
                            // 修正 维度1（新订单）的结果
                            BigDecimal totalAmtDiff = totalAmount_2.subtract(totalAmount_1);
                            BigDecimal payAmtDiff = payAmount_2.subtract(payAmount_1);

                            out.collect(new DataBean(1, oid, totalAmtDiff, payAmtDiff, true));

                            // 修正 维度2（新待付）的结果
                            out.collect(new DataBean(2, oid, totalAmount_1.negate(), payAmount_1.negate(), false));

                            // 新增维度3
                            out.collect(new DataBean(3, oid, totalAmount_2, payAmount_2, true));
                        }


                        // 1 已付 -> 2 已发货
                        if (dDate != null && dDate.equals(today) && status_1 == 1 && status_2 == 2) {
                            // 新增维度4
                            out.collect(new DataBean(4, oid, totalAmount_2, payAmount_2, true));
                        }


                        // 2 发货 -> 3 确认
                        if (cfDate != null && cfDate.equals(today) && status_1 == 2 && status_2 == 3) {
                            // 新增维度5
                            out.collect(new DataBean(5, oid, totalAmount_2, payAmount_2, true));
                        }

                    default:

                }

            }
        }).setParallelism(2);

        // 累计聚合
        SingleOutputStreamOperator<String> resultStream = dataBeanStream.global().process(new CalcProcessFunction()).setParallelism(1);
        resultStream.print().setParallelism(1);

        env.execute();

    }


    /**
     * 为什么要让我们的ProcessFunction实现CheckpointedFunction接口？
     * 因为我们需要用 OperatorState（算子状态）
     */
    public static class CalcProcessFunction extends ProcessFunction<DataBean, String> implements CheckpointedFunction {

        BigDecimal zero = BigDecimal.ZERO;
        Agg agg = new Agg();

        JSONObject jsonObject = new JSONObject();

        ListState<Agg> aggState;

        @Override
        public void processElement(DataBean dataBean, ProcessFunction<DataBean, String>.Context ctx, Collector<String> out) throws Exception {

            int dim = dataBean.dim;
            boolean isAddId = dataBean.isAddId;
            switch (dim) {
                case 1:
                    agg.orderTotalAmt = agg.orderTotalAmt.add(dataBean.totalAmt);
                    agg.orderTotalPayAmt = agg.orderTotalPayAmt.add(dataBean.payAmt);
                    if (isAddId) {
                        agg.orderTotalCntBm.add(dataBean.oid);
                    } else {
                        agg.orderTotalCntBm.remove(dataBean.oid);
                    }
                    break;

                case 2:
                    agg.toPayAmt = agg.toPayAmt.add(dataBean.payAmt);
                    if (isAddId) {
                        agg.toPayCntBm.add(dataBean.oid);
                    } else {
                        agg.toPayCntBm.remove(dataBean.oid);
                    }
                    break;

                case 3:
                    agg.payedAmt = agg.payedAmt.add(dataBean.payAmt);
                    if (isAddId) {
                        agg.payedCntBm.add(dataBean.oid);
                    } else {
                        agg.payedCntBm.remove(dataBean.oid);
                    }
                    break;

                case 4:
                    agg.deliveredAmt = agg.deliveredAmt.add(dataBean.payAmt);
                    if (isAddId) {
                        agg.deliveredCntBm.add(dataBean.oid);
                    } else {
                        agg.deliveredCntBm.remove(dataBean.oid);
                    }
                    break;

                case 5:

                    agg.confirmedAmt = agg.confirmedAmt.add(dataBean.payAmt);
                    if (isAddId) {
                        agg.confirmedCntBm.add(dataBean.oid);
                    } else {
                        agg.confirmedCntBm.remove(dataBean.oid);
                    }
                    break;
                default:
            }



            jsonObject.put("新订单总数",agg.orderTotalCntBm.getCardinality());
            jsonObject.put("新订单总额",agg.orderTotalAmt);
            jsonObject.put("新订单总应付额",agg.orderTotalPayAmt);

            jsonObject.put("新待付总数",agg.toPayCntBm.getCardinality());
            jsonObject.put("新待付总额",agg.toPayAmt);

            jsonObject.put("今日支付总数",agg.payedCntBm.getCardinality());
            jsonObject.put("今日支付总额",agg.payedAmt);

            jsonObject.put("今日发货总数",agg.deliveredCntBm.getCardinality());
            jsonObject.put("今日发货总额",agg.deliveredAmt);


            jsonObject.put("今日确认总数",agg.confirmedCntBm.getCardinality());
            jsonObject.put("今日确认总额",agg.confirmedAmt);

            out.collect(jsonObject.toJSONString());

        }


        /**
         * 系统要对状态做快照了，它会调用该方法；
         * 提供一个机会给用户： 抓紧更新你的状态数据吧
         */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            aggState.update(Collections.singletonList(agg));
        }

        /**
         * 系统重启了，已经加载好了之前该算子的状态
         * 会调用一下本函数的该方法，提供一个机会给用户： 恢复的状态数据给你了，你要拿它做点什么，赶紧做
         */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {

            aggState = context.getOperatorStateStore().getListState(new ListStateDescriptor<Agg>("agg_state", Agg.class));
            Iterator<Agg> iterator = aggState.get().iterator();

            if (iterator.hasNext()) {
                agg = iterator.next();
            }
        }

    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Agg implements Serializable {

        // 维度类型1:
        private RoaringBitmap orderTotalCntBm = RoaringBitmap.bitmapOf();
        private BigDecimal orderTotalAmt = BigDecimal.ZERO;
        private BigDecimal orderTotalPayAmt = BigDecimal.ZERO;

        // 维度类型2：
        private RoaringBitmap toPayCntBm = RoaringBitmap.bitmapOf();
        private BigDecimal toPayAmt = BigDecimal.ZERO;


        // 维度类型3：
        private RoaringBitmap payedCntBm = RoaringBitmap.bitmapOf();
        private BigDecimal payedAmt = BigDecimal.ZERO;


        // 维度类型4：
        private RoaringBitmap deliveredCntBm = RoaringBitmap.bitmapOf();
        private BigDecimal deliveredAmt = BigDecimal.ZERO;

        // 维度类型5:
        private RoaringBitmap confirmedCntBm = RoaringBitmap.bitmapOf();
        private BigDecimal confirmedAmt = BigDecimal.ZERO;


    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DataBean implements Serializable {
        private int dim;
        private int oid;
        private BigDecimal totalAmt;
        private BigDecimal payAmt;
        private boolean isAddId;
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OrderInfo implements Serializable {
        private int id;
        private int status;

        private BigDecimal total_amount;
        private BigDecimal pay_amount;

        private LocalDateTime create_time;
        private LocalDateTime payment_time;
        private LocalDateTime delivery_time;
        private LocalDateTime confirm_time;

    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CdcBean implements Serializable {
        private OrderInfo before;
        private OrderInfo after;
        private String op;

    }
}
