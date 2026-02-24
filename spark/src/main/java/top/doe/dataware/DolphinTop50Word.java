package top.doe.dataware;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;

public class DolphinTop50Word {

    public static void main(String[] args) throws AnalysisException {

        SparkSession spark = SparkSession.builder()
                .appName("DolphinTop50Word")
                .master("local")
                .enableHiveSupport()
                .getOrCreate();


        String dt = args[0];
        Dataset<Row> df = spark.read().table("tmp.dwd_dolphin_test").where("dt='" + dt + "'");

        JavaRDD<Word> wordJavaRDD = df.toJavaRDD()
                .mapPartitions(new FlatMapFunction<Iterator<Row>, Word>() {
                    @Override
                    public Iterator<Word> call(Iterator<Row> rowIterator) throws Exception {

                        StringReader sr = new StringReader("");
                        IKSegmenter ikSegmenter = new IKSegmenter(sr, true);


                        ArrayList<Word> words = new ArrayList<>();
                        while (rowIterator.hasNext()) {
                            Row row = rowIterator.next();
                            int id = row.getAs("id");
                            String title = row.getAs("title");

                            sr = new StringReader(title);
                            ikSegmenter.reset(sr);

                            Lexeme lexeme = null;
                            while ((lexeme = ikSegmenter.next()) != null) {

                                // 单字词抛弃
                                String w = lexeme.getLexemeText();
                                if (w.length() > 1) {
                                    words.add(new Word(id, w));
                                }
                            }

                        }

                        return words.iterator();
                    }
                });


        Dataset<Row> resDf = spark.createDataFrame(wordJavaRDD, Word.class);

        resDf.createTempView("tmp");

        spark.sql("insert into table tmp.dws_dolphin_test partition(dt='" + dt + "')" +
                "select  \n" +
                "id,word, row_number() over(partition by id order by count(1) desc ) as rk  \n" +
                "from tmp  \n" +
                "group by id,word");



        spark.close();
    }




    public static class Word implements Serializable {
        private int id;
        private String word;

        public Word() {
        }

        public Word(int id, String word) {
            this.id = id;
            this.word = word;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }
    }




}
