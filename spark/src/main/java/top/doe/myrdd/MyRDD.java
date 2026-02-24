package top.doe.myrdd;

import java.io.*;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class MyRDD {
    private Iterator<String> iter;
    private MyRDD preRdd;

    public MyRDD(MyRDD preRdd, Iterator<String> iter) {
        this.iter = iter;
        this.preRdd = preRdd;
    }


    public static MyRDD textFile(String path) throws Exception {

        Iterator<String> newIter = new Iterator<String>() {

            BufferedReader br = new BufferedReader(new FileReader(path));
            String line = br.readLine();
            boolean flag = false;

            @Override
            public boolean hasNext() {
                flag = true;
                return line != null;
            }

            @Override
            public String next() {

                if (flag) {

                    String rt = line;
                    try {
                        line = br.readLine();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    return rt;
                } else {
                    throw new RuntimeException("你不按规矩来....");
                }
            }
        };

        return new MyRDD(null,newIter);

    }


    public MyRDD filter(Predicate<String> f) {

        Stream<String> stream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(iter, Spliterator.ORDERED), false);
        Iterator<String> newIter = stream.filter(f).iterator();

        return new MyRDD(this, newIter);

    }


    public MyRDD map(Function<String, String> f) {

        Stream<String> stream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(iter, Spliterator.ORDERED), false);
        Iterator<String> newIter = stream.map(f).iterator();

        return new MyRDD(this, newIter);

    }


    public void saveAsTextFile(String path) throws IOException {

        BufferedWriter bw = new BufferedWriter(new FileWriter(path + "/part-00000"));

        while(iter.hasNext()){
            String next = iter.next();
            bw.write(next);
            bw.newLine();
        }

        bw.close();
    }



}
