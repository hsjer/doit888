package top.doe.mr.map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import top.doe.mr.formats.InputFormat;
import top.doe.mr.formats.RecordReader;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class MapTask implements Runnable {

    private Mapper userMapper;
    private String jobInfoJson;
    RecordReader recordReader;
    BufferedWriter bw;

    public MapTask(Mapper userMapper, String jobInfoJson, boolean hasReduce) throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {

        /**
         * inputInfo:
         * {
         *     "input_format":"top.doe.mr.formats.FileInputFormat",
         *     "input_path":"hdfs://namenode:8020/aa/input",
         *     "map_task_splits":[
         *          {
         *              "task_id":0,
         *              "split_start":1,
         *              "split_end":100
         *          },
         *          {
         *              "task_id":1,
         *              "split_start":101,
         *              "split_end":200
         *          }
         *     ],
         *     "db_param":{
         *         "jdbc_url":"jdbc:mysql://localhost:3306/test",
         *         "user_name":"root",
         *         "password":"123456",
         *         "db_name":"db_mall",
         *         "table_name":"order",
         *         "column_names":["id","name","age"]
         *     },
         *     "job_id":"job_2983759235",
         *     "task_id":"1"
         * }
         */
        this.userMapper = userMapper;
        this.jobInfoJson = jobInfoJson;

        // 根据作业参数信息，获取作业的InputFormat类
        JSONObject obj = JSON.parseObject(this.jobInfoJson);
        String inputFormatClassName = obj.getString("input_format");

        // 实例化InputFormat类
        Class<?> aClass = Class.forName(inputFormatClassName);
        InputFormat inputFormat = (InputFormat) aClass.newInstance();

        // 利用InputFormat实例对象，创建RecordReader的实例对象
        // { "input_path":"", "split":[1,100] }
        recordReader = inputFormat.createRecordReader(this.jobInfoJson);


        // 构造结果输出器
        String jobId = obj.getString("job_id");
        int taskId = obj.getIntValue("task_id");
        bw = new BufferedWriter(new FileWriter(jobId + "-" + taskId));

    }

    @Override
    public void run() {

        // 读取数据
        while (recordReader.hasNext()) {

            // 获取一行数据
            String data = recordReader.next();

            // 调用户的mapper实例
            String result = userMapper.map(data);

            // 将结果写出
            //     如果有reduce阶段，则将结果写到临时文件中，将来交给reduceTask
            //     如果没有reduce阶段，则map的结果，就是最终结果，写到job的结果输出路径去

            try {
                bw.write(result);
                bw.newLine();
                bw.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            bw.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
