package doe;

import doe.janino.inter.Person;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.SimpleCompiler;

public class JaninoTest {

    public static void main(String[] args) throws CompileException, ClassNotFoundException, InstantiationException, IllegalAccessException {

        SimpleCompiler simpleCompiler = new SimpleCompiler();
        simpleCompiler.setParentClassLoader(SimpleCompiler.class.getClassLoader());

        String code = "import doe.janino.inter.Person;\n" +
                "\n" +
                "public class Student implements Person {\n" +
                "\n" +
                "    @Override\n" +
                "    public void initialize() {\n" +
                "        System.out.println(\"我初始化了....\");\n" +
                "    }\n" +
                "\n" +
                "    @Override\n" +
                "    public void calculate() {\n" +
                "        System.out.println(\"我运算了....\");\n" +
                "    }\n" +
                "}";

        simpleCompiler.cook(code);

        Class<?> studentClass = simpleCompiler.getClassLoader().loadClass("Student");

        Person student = (Person) studentClass.newInstance();

        student.initialize();
        student.calculate();

    }


}
