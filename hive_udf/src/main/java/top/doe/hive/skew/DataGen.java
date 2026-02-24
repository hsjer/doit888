package top.doe.hive.skew;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.math.RandomUtils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class DataGen {

    public static void main(String[] args) throws IOException {
        //genEvents();
        genUsers();
    }


    public static  void genEvents() throws IOException {

        BufferedWriter bw = new BufferedWriter(new FileWriter("./data/events.txt"));

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < 10000000; i++) {  // 500w
            int id = RandomUtils.nextInt(2000000); // 200w

            int rd = RandomUtils.nextInt(10);
            if(rd<8) id = 333;   // 1000w * 0.8 = 800w的333, 200w其他

            sb.append(id).append(",");

            sb.append(RandomStringUtils.randomAlphabetic(10)).append(",");
            sb.append(RandomStringUtils.randomAlphabetic(10)).append(",");
            sb.append(RandomStringUtils.randomAlphabetic(10)).append(",");
            sb.append(RandomStringUtils.randomAlphabetic(10)).append(",");
            sb.append(RandomStringUtils.randomNumeric(10)).append(",");
            sb.append(RandomStringUtils.randomNumeric(10)).append(",");
            sb.append(RandomStringUtils.randomNumeric(10)).append(",");
            sb.append(RandomStringUtils.randomNumeric(10)).append("\n");

            bw.write(sb.toString());

            sb.delete(0,sb.length());
        }
        bw.flush();
        bw.close();

    }


    public static  void genUsers() throws IOException {

        BufferedWriter bw = new BufferedWriter(new FileWriter("./data/users.txt"));

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < 2000000; i++) {

            sb.append(i).append(",");
            sb.append(RandomStringUtils.randomAlphabetic(10)).append(",");
            sb.append(RandomStringUtils.randomAlphabetic(10)).append(",");
            sb.append(RandomStringUtils.randomAlphabetic(10)).append(",");
            sb.append(RandomStringUtils.randomAlphabetic(10)).append(",");
            sb.append(RandomStringUtils.randomNumeric(10)).append(",");
            sb.append(RandomStringUtils.randomNumeric(10)).append(",");
            sb.append(RandomStringUtils.randomNumeric(10)).append(",");
            sb.append(RandomStringUtils.randomNumeric(10)).append("\n");

            bw.write(sb.toString());

            sb.delete(0,sb.length());
        }
        bw.flush();
        bw.close();

    }


}
