package learn.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {

    /**
     * A program for counting the number of repitition of words
     * @param args No usage of Commandline arguments
     */
    public static void main(String[] args) {

        JavaSparkContext jsc = new JavaSparkContext(new SparkConf().setMaster("local[*]").setAppName("Word Count"));

        JavaRDD<String> lines = jsc.textFile("src\\main\\resources\\WordCountInput.txt");

        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split("\\s+")).iterator());

        JavaPairRDD<String, Integer> wordPairs = words.mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> wordsWithCount = wordPairs.reduceByKey((integer1, integer2) -> integer1 + integer2);

        System.out.println(wordsWithCount.collect());

    }

}
