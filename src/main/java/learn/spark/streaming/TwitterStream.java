package learn.spark.streaming;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.*;
import twitter4j.Status;

import java.io.Serializable;
import java.util.Arrays;

public class TwitterStream {

    /**
     * A program which streams and displays only fresh tweets based on topics given from twitter
     * @param args List of topics to fetch tweets from
     */
    public static void main(String[] args){

        if(args.length < 6){
            System.out.println("Usage : TwitterStream <consumerKey> <consumerSecret> <accessToken> <accessTokenSecret> <space separated topic list>");
            System.exit(0);
        }

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getLogger("TwitterStreamImpl").setLevel(Level.OFF);

        String consumerKey = args[0];
        String consumerSecret = args[1];
        String accessToken = args[2];
        String accessSecret = args[3];

        //Twitter App Credentials passed from commandline
        System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
        System.setProperty("twitter4j.oauth.accessToken", accessToken);
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessSecret);

        String[] topics = Arrays.copyOfRange(args, 5, args.length);

        SparkConf sparkConf = new SparkConf().setAppName("TwitterStream").setMaster("local[*]");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        JavaStreamingContext jssc = new JavaStreamingContext(jsc, new Duration(2000));

        JavaReceiverInputDStream<Status> dataStream = TwitterUtils.createStream(jssc, topics);

        JavaDStream<String> tweets = dataStream.map(status -> status.getText());

        JavaDStream<String> FreshTweets = tweets.filter(text -> !text.startsWith("RT"));

        FreshTweets.foreachRDD(tweet -> {
            if(tweet.count() > 0){
                tweet.foreach(t -> System.out.println(t));
            }
        });

        jssc.start();

        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
