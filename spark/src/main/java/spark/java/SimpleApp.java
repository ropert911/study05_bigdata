package spark.java;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleApp {
    private static Logger logger = LoggerFactory.getLogger(SimpleApp.class);

    public static void main(String[] args) {
        String logFile = "file:///opt/spark-2.3.1-bin-hadoop2.7/README.md"; // Should be some file on your system
        JavaSparkContext sc = new JavaSparkContext("local", "Simple App",
                "file:///opt/apache-hive-1.2.2-bin/", new String[]{"target/simple-project-1.0.jar"});
        JavaRDD<String> logData = sc.textFile(logFile).cache();

        long numAs = logData.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) {
                return s.contains("a");
            }
        }).count();

        long numBs = logData.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) {
                return s.contains("b");
            }
        }).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
    }
}