package top.doe;

import com.alibaba.fastjson.JSONObject;
import top.doe.other.SomeTask;

public class HelloWorld {
    public static void main(String[] args) throws InterruptedException {

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("name", "doe");
        jsonObject.put("age", 18);
        System.out.println(jsonObject.toJSONString());


        int cnt = 0;
        try{
            cnt = Integer.parseInt(args[0]);
        }catch (Exception e){
            System.err.println("参数传递错误");
            System.exit(1);
        }

        SomeTask someTask = new SomeTask();

        someTask.run(cnt);


    }
}
