package top.doe.utils;

import com.alibaba.fastjson.JSONArray;
import top.doe.exceptions.OperatorNotSupportException;


public class EventCompareUtil {

    public static boolean compareEventProp(String op, JSONArray pValueArray, String curPValueStr) throws OperatorNotSupportException {
        switch (op){
            case ">":
                Double targetValue = pValueArray.getDouble(0);
                double realValue = Double.parseDouble(curPValueStr);
                return realValue > targetValue;

            case ">=":
                Double targetValue2 = pValueArray.getDouble(0);
                double realValue2 = Double.parseDouble(curPValueStr);
                return realValue2 >= targetValue2;

            case "<":
                Double targetValue3 = pValueArray.getDouble(0);
                double realValue3 = Double.parseDouble(curPValueStr);
                return realValue3 >= targetValue3;

            case "<=":
                Double targetValue4 = pValueArray.getDouble(0);
                double realValue4 = Double.parseDouble(curPValueStr);
                return realValue4 >= targetValue4;

            case "!=":
                return !pValueArray.getString(0).equals(curPValueStr);
            case "=":
                return pValueArray.getString(0).equals(curPValueStr);

            case "contains":
                return curPValueStr.contains(pValueArray.getString(0));

            case "between":
                Double targetValue51 = pValueArray.getDouble(0);
                Double targetValue52 = pValueArray.getDouble(1);
                double realValue5 = Double.parseDouble(curPValueStr);
                return  realValue5 >= targetValue51 && realValue5 <= targetValue52;

            default:
                throw new OperatorNotSupportException("系统不支持条件运算符：" + op + ",可选的运算符有:>,>=,<,<=,=,!=,between,contains");

        }
    }

}
