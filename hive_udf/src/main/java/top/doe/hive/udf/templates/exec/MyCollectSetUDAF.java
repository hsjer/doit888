package top.doe.hive.udf.templates.exec;

import org.apache.arrow.flatbuf.Int;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.BytesWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

public class MyCollectSetUDAF extends UDAF {


    public static class MyCollectSetUDAFEvaluator implements UDAFEvaluator {

        private HashSet<Integer> agg;

        public MyCollectSetUDAFEvaluator(){
            init();
        }

        @Override
        public void init() {
            agg = new HashSet<>();
        }

        public boolean iterate(Integer id){

            agg.add(id);

            return true;
        }


        public ArrayList<Integer> terminatePartial(){

            ArrayList<Integer> lst = new ArrayList<>();
            lst.addAll(this.agg);

            return lst;
        }

        public boolean merge(ArrayList<Integer> partialLst) throws IOException {

            this.agg.addAll(partialLst);

            return true;
        }


        // 返回的list，在sql中对应的类型就是sql中的 数组
        public ArrayList<Integer> terminate() throws IOException {

            ArrayList<Integer> resLst  = new ArrayList<>();
            resLst.addAll(this.agg);

            return resLst;
        }


    }



}
