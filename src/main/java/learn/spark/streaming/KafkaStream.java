package learn.spark.streaming;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;


import java.util.*;

public class KafkaStream {

    /**
     * A program which streams and processes the messages from apache kafka
     * @param args List of kafka topics to stream the messages
     */
    public static void main(String[] args){

        if(args.length < 2){
            System.out.println("Usage : KafkaStream <Interval> <space separated topic list>");
            System.exit(0);
        }

        SparkConf sparkConf = new SparkConf().setAppName("test").setMaster("local[*]");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        JavaStreamingContext jssc = new JavaStreamingContext(jsc, new Duration(Long.parseLong(args[0])));

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "localhost:9092");

        Set<String> topics = new HashSet<>();

        for(int i=1; i<args.length; i++)
            topics.add(args[i]);

        JavaPairInputDStream<String, String> input = KafkaUtils.createDirectStream(jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics);


        JavaDStream<String> ideMessages = input.map(tuple -> tuple._2);

        ideMessages.foreachRDD(System.out::println);

        jssc.start();

        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
