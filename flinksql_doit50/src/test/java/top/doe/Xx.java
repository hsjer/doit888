package top.doe;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.ArrayList;
import java.util.List;

public class Xx {
    public static void main(String[] args) {
        String text = "[1,{\"score\":80,\"course\":\"化学\"},{\"score\":90,\"course\":\"物理\"}]";
        String regex = "(\\d+)|(\\{[^}]+\\})";

        // Compile the pattern
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(text);

        // Store matches in a list
        List<String> results = new ArrayList<>();

        while (matcher.find()) {
            // Check which group matched and add it to results
            if (matcher.group(1) != null) {
                results.add(matcher.group(1)); // Group 1: the number
            } else if (matcher.group(2) != null) {
                results.add(matcher.group(2)); // Group 2: each JSON string
            }
        }

        // Print the results
        System.out.println(results);
    }
}

