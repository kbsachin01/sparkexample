
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * Created by sachin on 2/9/17.
 */
public class SparkAppMain {
    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf()
                .setAppName("Example Spark App")
                .setMaster("local[*]"); // Delete this line when submitting to a cluster
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> stringLinesJavaRDD = sparkContext.textFile("/home/sachin/Desktop/Datasets/exp.csv");
        final String regex = "'[0-9]+','[0-9]+',";
        JavaRDD<String> filteredLinesJavaRDD = stringLinesJavaRDD.filter(new Function<String, Boolean>() {
            public Boolean call(String x) throws Exception {
                return x.matches(regex);
            }
        });
        filteredLinesJavaRDD.saveAsTextFile("/home/sachin/Desktop/Datasets/expFiltered.csv");
        System.out.println("Test = " + filteredLinesJavaRDD.count());
        System.out.println("Number of lines in file = " + stringLinesJavaRDD.count());

    }
}
