package top.doe;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestRegex {
    public static void main(String[] args) {


//        String input = "[2024-09-25,16:30:30],江西,{\"uid\":1,\"oid\":\"o_1\",\"pid\":1,\"amt\":78.8}";
          String input = "12,[{\"event_id\":\"c\",\"action_time\":1727232579000},{\"event_id\":\"a\",\"action_time\":1727232579000}]";

        // 定义正则表达式
        String regex = "(\\d+),(.*)";

        // 编译正则表达式
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(input);

        // 如果匹配成功
        if (matcher.find()) {
            // 提取并打印匹配到的部分
            String part1 = matcher.group(1);  // [2024-09-25,16:30:30]
            String part2 = matcher.group(2);  // 江西

            System.out.println(part1);
            System.out.println(part2);
        } else {
            System.out.println("没有匹配到预期的部分");
        }
    }
}

