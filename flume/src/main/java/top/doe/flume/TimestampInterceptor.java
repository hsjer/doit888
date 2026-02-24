package top.doe.flume;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;

public class TimestampInterceptor implements Interceptor {


    private String timeKey;

    public TimestampInterceptor(String timeKey) {
        this.timeKey = timeKey;
    }

    // 对象初始化
    @Override
    public void initialize() {

    }

    // 数据拦截处理的功能，一次处理一条
    @Override
    public Event intercept(Event event) {

        // 从我们自己的行为日志中，抽取用户的行为时间，放入event的header中去
        byte[] body = event.getBody();
        String json = new String(body);

        try {
            JSONObject jsonObject = JSON.parseObject(json);
            String ts = jsonObject.getString(timeKey);

            // 把抽取到的行为时间，放入event的header中
            event.getHeaders().put("timestamp", ts);
            event.getHeaders().put("flag","normal");
        }catch (Exception e){

            // 如果json解析失败，则往event的header中放入一个标记
            event.getHeaders().put("flag","malformed");
            event.getHeaders().put("timestamp",System.currentTimeMillis()+"");

        }
        return event;
    }

    // 数据拦截处理功能，一次处理一批
    @Override
    public List<Event> intercept(List<Event> list) {

        for (Event event : list) {
            intercept(event);
        }

        return list;
    }

    // 生命周期的结束方法
    @Override
    public void close() {

    }


    /**
     * 构造拦截器实例的构建器
     */
    public static class MyBuilder implements Interceptor.Builder{
        String timeKey;
        @Override  // 构造一个拦截器的实例对象
        public Interceptor build() {
            return new TimestampInterceptor(timeKey);
        }

        @Override  // context会传入配置文件中配置的自定义参数
        public void configure(Context context) {
            timeKey = context.getString("time_key", "action_time");

        }
    }

}
