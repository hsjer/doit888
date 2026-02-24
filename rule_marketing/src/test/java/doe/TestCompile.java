//package doe;
//
//import com.alibaba.fastjson.JSONObject;
//import org.codehaus.commons.compiler.CompileException;
//import org.codehaus.janino.SimpleCompiler;
//import top.doe.intf.RuleCalculator;
//
//public class TestCompile {
//
//    public static void main(String[] args) throws ClassNotFoundException, InstantiationException, IllegalAccessException, CompileException {
//
//        String code = "import top.doe.intf.RuleCalculator;" +
//                "import com.alibaba.fastjson.JSONObject;" +
//                "public class RuleCalculatorImpl implements RuleCalculator {\n" +
//                "    @Override\n" +
//                "    public void init(String param, JSONObject jsonObject) {\n" +
//                "\n" +
//                "        System.out.println(\"初始化了...\" + param+\" ==> \" + jsonObject.toJSONString());\n" +
//                "\n" +
//                "    }\n" +
//                "\n" +
//                "    @Override\n" +
//                "    public void calc(String event) {\n" +
//                "        System.out.println(\"计算了...\" + event);\n" +
//                "\n" +
//                "    }\n" +
//                "}";
//
//
//        SimpleCompiler compiler = new SimpleCompiler();
//        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
//        compiler.setParentClassLoader(classLoader);
//        compiler.cook(code);
//
//        Class<RuleCalculator> aClass = (Class<RuleCalculator>) compiler.getClassLoader().loadClass("RuleCalculatorImpl");
//
//        RuleCalculator ruleCalculator = aClass.newInstance();
//
//        JSONObject jsonObject = new JSONObject();
//        jsonObject.put("a",3);
//        jsonObject.put("b",4);
//
//        ruleCalculator.init("haha",jsonObject);
//        ruleCalculator.calc("heihei");
//
//
//    }
//}
