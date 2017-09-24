import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;

/**
 * Created by sachin on 14/9/17.
 */
public class TwitterStreaming {

    public static void main(String[] args) throws InterruptedException {

        final String OUTPUT_LOCATION = "/home/sachin/Desktop/TwitterStreamData/";

        final String CONSUMER_API_KEY = args[0];
        final String CONSUMER_API_SECRET = args[1];
        final String ACCESS_TOKEN = args[2];
        final String ACCESS_TOKEN_SECRET = args[3];

        System.setProperty("twitter4j.oauth.consumerKey", CONSUMER_API_KEY);
        System.setProperty("twitter4j.oauth.consumerSecret", CONSUMER_API_SECRET);
        System.setProperty("twitter4j.oauth.accessToken", ACCESS_TOKEN);
        System.setProperty("twitter4j.oauth.accessTokenSecret", ACCESS_TOKEN_SECRET);

        SparkConf conf = new SparkConf().setAppName("Twitter Analysis").setMaster("local[*]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(2000));


        JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc);

        JavaDStream tweetsInEnglishStream = twitterStream.filter(new Function<Status, Boolean>() {
            public Boolean call(Status x) throws Exception {
                return x.getLang().equals("en");
            }
        }).filter(
                new Function<Status, Boolean>() {
                    public Boolean call(Status status) {
                        return status.getGeoLocation() != null;
                    }
                }
        );

        JavaDStream<String> statuses = tweetsInEnglishStream.map(
                new Function<Status, String>() {
                    public String call(Status status) {
                        return status.getCreatedAt().toString() + ": " + status.getText();
                    }
                }
        );

        statuses.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            public void call(JavaRDD<String> statusJavaRDD) throws Exception {
                statusJavaRDD.saveAsTextFile("/home/sachin/Desktop/TwitterStreamData/");
            }
        });

        statuses.print();
        jssc.start();
        jssc.awaitTermination();

    }
}
