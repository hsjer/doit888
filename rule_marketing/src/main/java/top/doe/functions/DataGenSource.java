package top.doe.functions;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class DataGenSource implements SourceFunction<String> {

    ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<String>(1000000);

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        Thread.sleep(6000);

        JSONObject data = new JSONObject();
        JSONObject props = new JSONObject();

        String[] events = {"A", "B", "C", "D", "E", "F", "G", "H", "I", "K", "K", "1", "11", "2", "22", "3", "33", "4", "232", "133", "323", "44"};

        long start = System.currentTimeMillis();
        log.warn("source准备开始生成数据,此刻时间:{}", DateFormatUtils.format(new Date(start), "yyyy-MM-dd HH:mm:ss.SSS"));
        data.put("user_id", 0L);
        data.put("event_id", "START");
        data.put("action_time", start);

        props.put("p1", String.valueOf(1));
        data.put("properties", props);
        sourceContext.collect(data.toJSONString());


        // 线程数
        int th = 12;
        int lines = 100000;

        //int th = 1;
        //int lines = 1;

        CountDownLatch latch = new CountDownLatch(th);
        for (int i = 0; i < th; i++) {
            // 启动数据生产线程
            new Thread(new DataRun(events, lines, queue, latch)).start();
            Thread.sleep(100);
        }

        // 启动数据消费线程
        new Thread(new Runnable() {
            @Override
            public void run() {
                int cnt=0;
                while (true) {
                    String d =  queue.poll();
                    if (d != null) {
                        //synchronized (sourceContext.getCheckpointLock()) {
                            sourceContext.collect(d);
                            cnt++;
                            if (d.contains("END")) {
                                log.warn("发送线程,取到END事件并传给了下游,总发送数据条数:{}",cnt);
                            }
                        //}
                    }
                }
            }
        }).start();

        latch.await();
        log.warn("线程数据生成完成,准备生成结束标记事件");

        data.put("user_id", 0L);
        data.put("event_id", "END");

        long end = System.currentTimeMillis();
        data.put("action_time", end);

        props.put("p1", String.valueOf(1));
        data.put("properties", props);
        queue.put(data.toJSONString());

        log.warn("source结束生成数据,此刻时间:{},总耗时:{}", end, (end - start) / 1000);
        while (true) {
            Thread.sleep(Long.MAX_VALUE);
        }
    }

    @Override
    public void cancel() {

    }


    public static class DataRun implements Runnable {
        String[] events;
        ArrayBlockingQueue<String> queue;
        CountDownLatch latch;
        int lines;

        public DataRun(String[] events, int lines, ArrayBlockingQueue<String> queue, CountDownLatch latch) {
            this.events = events;
            this.queue = queue;
            this.latch = latch;
            this.lines = lines;
        }

        @Override
        public void run() {
            JSONObject data = new JSONObject();
            JSONObject props = new JSONObject();
            for (int i = 0; i < lines; i++) {
                // {"account":"zs","user_id":8,"event_id":"W","action_time":1732019110039,"properties":{"p1":"800"}}
                data.put("user_id", (long) RandomUtils.nextInt(30000, 45000));
                data.put("event_id", events[RandomUtils.nextInt(0, events.length)]);
                data.put("action_time", System.currentTimeMillis());

                props.put("p1", String.valueOf(RandomUtils.nextInt(1, 2000)));
                data.put("properties", props);

                try {
                    queue.put(data.toJSONString());
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

            }

            latch.countDown();

        }
    }


}
