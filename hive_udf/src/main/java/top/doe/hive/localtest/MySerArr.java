package top.doe.hive.localtest;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

import java.util.ArrayList;

public class MySerArr extends UDAF {

    public static class SerEvaluator implements UDAFEvaluator {

        Agg agg;

        public SerEvaluator() {
            super();
            init();
        }

        @Override
        public void init() {

            InnerObj innerObj = new InnerObj();

            agg = new Agg();
            agg.innerObj = innerObj;
        }


        public boolean iterate(Double value) {
            if (value != null) {
                agg.innerObj.sum += value;
                agg.innerObj.cnt += 1;

                agg.innerObj.eles.add(value);
            }
            return true;
        }


        public Agg terminatePartial() {
            return agg;
        }


        public boolean merge(Agg partialAgg) {
            if (partialAgg != null) {

                agg.innerObj.eles.addAll(partialAgg.innerObj.eles);

                agg.innerObj.sum += partialAgg.innerObj.sum + agg.chars.length;
                agg.innerObj.cnt += partialAgg.innerObj.eles.size();
            }

            return true;
        }


        public Double terminate() {
            System.out.println("terminate了,agg.innerObj.sum........."+agg.innerObj.sum);
            System.out.println("terminate了,agg.innerObj.eles........."+agg.innerObj.eles);
            return (double)agg.innerObj.sum/ agg.chars.length;
        }


        /**
         *
         */
        public static class Agg {
            double sum;
            int cnt;
            InnerObj innerObj;
            char[] chars = new char[1024];
            //List<Double> doubleList = new ArrayList<>();

        }


        public static class InnerObj{
            double non;
            double sum;
            int cnt;
            String some;
            ArrayList<Double> eles = new ArrayList<>();
        }


    }


}
