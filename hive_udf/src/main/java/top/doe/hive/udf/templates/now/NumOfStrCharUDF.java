package top.doe.hive.udf.templates.now;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

public class NumOfStrCharUDF extends GenericUDF {

    private StringObjectInspector stringOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // 校验参数长度
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("StringLengthUDF only takes 1 argument");
        }

        // 校验参数类型
        if (!(arguments[0] instanceof StringObjectInspector)) {
            throw new UDFArgumentTypeException(0, "Argument must be a string");
        }



        stringOI = (StringObjectInspector) arguments[0];

        // 返回函数的返回值类型
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        // 获取传入的字符串
        String input = stringOI.getPrimitiveJavaObject(arguments[0].get());

        // 返回字符串长度
        return input == null ? null : input.length();
    }

    @Override
    public String getDisplayString(String[] children) {
        return "StringLengthUDF(" + children[0] + ")";
    }
}
