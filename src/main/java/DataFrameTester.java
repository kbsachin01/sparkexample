import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by sachin on 6/9/17.
 */
public class DataFrameTester {

    public static void main(String[] args) {

        SparkSession ss = SparkSession.builder()
                .master("local")
                .appName("Dataframe Row Count")
                .config("spark.some.config.option", "some-value").getOrCreate();

        Dataset<Row> df = ss.read().csv("/home/sachin/Desktop/Datasets/unconverted.csv");

        df.printSchema();

        df.show();

    }
}
