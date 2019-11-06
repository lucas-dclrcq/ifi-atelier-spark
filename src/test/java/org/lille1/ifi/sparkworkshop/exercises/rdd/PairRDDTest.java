package org.lille1.ifi.sparkworkshop.exercises.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class PairRDDTest {
    private SparkConf sparkConf;
    private JavaSparkContext sparkContext;

    @BeforeAll
    void setup() {
        // We need to set the log level to warn in order to filter out uninteresting logging from the cluster
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        // Set up a simple local Spark Cluster
        this.sparkConf = new SparkConf()
                .setAppName("startingSpark") // The name of the cluster
                .setMaster("local[*]");

        // Set up a connection to the Spark cluster configured just above
        this.sparkContext = new JavaSparkContext(sparkConf);
    }

    @AfterAll
    void close() {
        // Don't forget to close the connection to the cluster after we're done
        this.sparkContext.close();
    }

    @Test
    void ex5_pairRdd() {
        final List<String> strings = Arrays.asList(
                "WARN Tuesday 4 September 2018",
                "WARN Tuesday 4 September 201"
        );
    }
}
