package top.doe.hive.udf.templates.now;


import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

@Description(name = "str", value = "_FUNC_(expr) - 返回该列中所有字符串的字符总数")
public class TotalNumOfStrCharUDAF extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly one argument is expected.");
        }
        //Serde实现数据序列化和反序列化以及提供一个辅助类ObjectInspector帮助使用者访问需要序列化或者反序列化的对象。
        //
        //Serde层构建在数据存储和执行引擎之间，实现数据存储+中间数据存储和执行引擎的解耦。
        //这里为什么提到数据存储和中间数据存储两个概念，因为数据序列化和反序列化不仅仅用在对目标文件的读取和结果数据写入，
        // 还需要实现中间结果保存和传输，hive最终会将SQL转化为mapreduce程序，而mapreduce程序需要读取原始数据，并将最终的结果数据写入存储介质，
        // Serde一方面用在针对inputformat中RecordReader读取数据的解析和最终结果的保存，另一方面，在map和reduce之间有一层shuffle，
        // 中间结果由hadoop完成shuffle后也需要读取并反序列化成内部的object，这个object实际上通常是一个Array或者list，
        // 但hive会提供一个StandardStructObjectInspector给用户进行该Object的访问。

        //作用主要是解耦数据使用与数据格式，使得数据流在输入输出端切换不同的输入输出格式，不同的Operator上使用不同的格式。
        ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(parameters[0]);
        //getCategory()方法获取ObjectInspector 对象的类型
        System.out.println("getCategory()方法获取ObjectInspector 对象的类型");
        System.out.println(oi.getCategory().name());
        if (oi.getCategory() != ObjectInspector.Category.PRIMITIVE){
            throw new UDFArgumentTypeException(0,
                    "Argument must be PRIMITIVE, but "
                            + oi.getCategory().name()
                            + " was passed.");
        }

        //将ObjectInspector 对象强制转换为PrimitiveObjectInspector
        PrimitiveObjectInspector inputOI = (PrimitiveObjectInspector) oi;
        //getCategory()方法获取PrimitiveObjectInspector 对象的类型
        System.out.println("getCategory()方法获取PrimitiveObjectInspector 对象的类型");
        System.out.println(inputOI.getCategory().name());
        if (inputOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING){
            throw new UDFArgumentTypeException(0,
                    "Argument must be String, but "
                            + inputOI.getPrimitiveCategory().name()
                            + " was passed.");
        }

        return new TotalNumOfLettersEvaluator();
    }
    //定义一个新的类，用于继承GenericUDAFEvaluator，来使用UDAF操作
    public static class TotalNumOfLettersEvaluator extends GenericUDAFEvaluator {

        PrimitiveObjectInspector inputOI;
        ObjectInspector outputOI;
        PrimitiveObjectInspector integerOI;

        int total = 0;
        //每个子类都应该覆盖这个函数
        //子类应该调用super。初始化(m，参数)以获得模式设置。
        // 确定各个阶段输入输出参数的数据格式ObjectInspectors
        // 并且注意，init()的调用不是单次的，是多次的。
        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            assert (parameters.length == 1);
            super.init(m, parameters);

            //map阶段读取sql列，输入为String基础数据格式
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                inputOI = (PrimitiveObjectInspector) parameters[0];
            } else {
                //其余阶段，输入为Integer基础数据格式
                integerOI = (PrimitiveObjectInspector) parameters[0];
            }

            // 指定各个阶段输出数据格式都为Integer类型
            outputOI = ObjectInspectorFactory.getReflectionObjectInspector(Integer.class,
                    ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
            return outputOI;

        }

        /**
         * 存储当前字符总数的类
         */
        //AggregationBuffer 允许我们保存中间结果，通过定义我们的buffer，
        // 我们可以处理任何格式的数据，在代码例子中字符总数保存在AggregationBuffer 。
        static class LetterSumAgg implements AggregationBuffer {
            int sum = 0;
            void add(int num){
                sum += num;
            }
        }
        // 保存数据聚集结果的类
        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            LetterSumAgg result = new LetterSumAgg();
            return result;
        }
        // 重置聚集结果
        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            LetterSumAgg aggBuffer = (LetterSumAgg) agg;
            aggBuffer.sum = 0;
        }

        private boolean warned = false;
        // map阶段，迭代处理输入sql传过来的列数据
        // 注意这里的迭代，当map阶段从表中读取一行时，就会调用一次iterate()方法，如果存在多行，就会调用多次。
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException {
            assert (parameters.length == 1);
            if (parameters[0] != null) {
                LetterSumAgg myagg = (LetterSumAgg) agg;
                //通过基本数据类型OI解析Object p的值
                Object p1 = ((PrimitiveObjectInspector) inputOI).getPrimitiveJavaObject(parameters[0]);
                myagg.add(String.valueOf(p1).length());
            }
        }

        // map与combiner结束返回结果，得到部分数据聚集结果
        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            LetterSumAgg myagg = (LetterSumAgg) agg;
            total += myagg.sum;
            return total;
        }
        // combiner合并map返回的结果，还有reducer合并mapper或combiner返回的结果。
        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {
            if (partial != null) {

                LetterSumAgg myagg1 = (LetterSumAgg) agg;

                Integer partialSum = (Integer) integerOI.getPrimitiveJavaObject(partial);

                LetterSumAgg myagg2 = new LetterSumAgg();

                myagg2.add(partialSum);
                myagg1.add(myagg2.sum);
            }
        }
        // reducer阶段，输出最终结果
        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            LetterSumAgg myagg = (LetterSumAgg) agg;
            total = myagg.sum;
            return myagg.sum;
        }

    }

}

