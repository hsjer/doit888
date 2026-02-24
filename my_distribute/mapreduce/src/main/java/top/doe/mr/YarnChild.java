package top.doe.mr;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import top.doe.mr.map.MapTask;
import top.doe.mr.map.Mapper;

import java.io.IOException;
import java.util.Map;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/8/18
 * @Desc: 学大数据，上多易教育
 *  是mapreduce分布式运算程序中真正的执行进程
 *  职责：
 *     根据参数，来执行 MapTask或者ReduceTask 逻辑
 *     负责与AppMaster之间的通信协调
 **/
public class YarnChild {

    public static void main(String[] args) throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {
        /* *
         * yarn child要成功运行处理逻辑，需要知道如下信息：
         *  1. 是执行map阶段的任务，还是执行reduce阶段的任务
         *  2. 执行map任务或者reduce任务时，用户的Mapper或Reducer实现类是哪个
         *  3. 本Task的JobId，TaskId
         *  4. 本task的输入数据文件路径和输出结果的文件路径
         */

        /* *
         * {
         *     "phase":"map",
         *     "userMapperName":"com.doit.mr.XMapper",
         *     "userReducerName":"com.doit.mr.XReducer",
         *     "job_id":"job_1928375928375",
         *     "task_id":"map-0",
         *     "input_path":"hdfs://namenode:8020/aa/input/",
         *     "output_path":"hdfs://namenode:8020/aa/output/"
         * }
         */
        String paramJson = args[0];
        // 解析json，获取所需的信息
        JSONObject jsonObject = JSON.parseObject(paramJson);
        String phase = jsonObject.getString("phase");
        if("map".equals(phase)){
            // 执行map阶段的任务

            // 从配置信息中获取用户的Mapper实现类名，并将它反射实例化
            String userMapperName = jsonObject.getString("userMapperName");
            Class<?> userMapperClass = Class.forName(userMapperName);
            Object userMapperObject = userMapperClass.newInstance();
            Mapper userMapper = null;
            if(userMapperObject instanceof Mapper){
                userMapper = (Mapper) userMapperObject;
            }else{
                throw new RuntimeException("userMapperObject 必须实现Mapper接口");
            }

            // mapTask在工作的时候要知道：输入数据的路径，和输出结果的路径
            //String inputPath = jsonObject.getString("input_path");
            String outputPath = jsonObject.getString("output_path");


            // 构造mapTask runnable对象，并用线程执行起来
            // mapTask在工作的时候必须有用户的mapper实现对象，还要有输入数据的路径，和输出结果的路径
            new Thread(new MapTask(userMapper,paramJson,false)).start();

        }else if("reduce".equals(phase)){
            // 执行reduce阶段的任务


        }else{
            throw new RuntimeException("不支持的phase");
        }

        System.out.println("负责跟app master之间的协调通信 ： 保持跟appmaster的心跳，定期汇报自身执行状态");

    }

}
