package top.doe.mapreduce.demos.demo4;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MapSideJoinMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    Map<Integer,JSONObject> userMap = new HashMap<>();

    @Override
    protected void setup(Mapper<LongWritable, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {

        // 读本地工作目录中的缓存文件（用户数据）
        BufferedReader br = new BufferedReader(new FileReader("user-info.txt"));
        String line;
        while ((line = br.readLine())!=null){
            // {"id":1001,"member_level":2,"gender":"male"}
            JSONObject userObj = JSON.parseObject(line);
            userMap.put(userObj.getInteger("id"),userObj );
        }

        br.close();

    }

    @Override  // 这里读入的只有订单数据
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {
        // {"order_id":"o1","member_id":1001,"amount":120.8,"receive_address":"beijing","date":"2024-08-1"}
        // {"order_id":"o2","member_id":1002,"amount":125.2,"receive_address":"beijing","date":"2024-08-1"}

        JSONObject orderObj = JSON.parseObject(value.toString());
        Integer userId = orderObj.getInteger("member_id");

        // 根据订单中的用户id，去用户数据hashmap中匹配他的用户信息
        JSONObject userObj = userMap.get(userId);

        // 把找到的用户信息字段，填充到 “订单数据”中去
        orderObj.put("gender",userObj.getString("gender"));
        orderObj.put("member_level",userObj.getIntValue("member_level"));

        // 输出关联结果
        context.write(NullWritable.get(),new Text(orderObj.toJSONString()));

    }
}
