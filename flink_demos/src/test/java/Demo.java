import org.apache.commons.lang3.time.DateFormatUtils;

public class Demo {
    public static void main(String[] args) {

        String window_start = DateFormatUtils.format(1729128970000L / (5 * 60 * 1000) * (5 * 60 * 1000), "yyyy-MM-dd HH:mm:ss.SSS");
        String window_end = DateFormatUtils.format((1729128970000L+5 * 60 * 1000) / (5 * 60 * 1000) * (5 * 60 * 1000), "yyyy-MM-dd HH:mm:ss.SSS");

        System.out.println(window_start);
        System.out.println(window_end);


    }
}
