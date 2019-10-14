package org.lille1.ifi.sparkworkshop.config;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class LocalSparkExercise implements AutoCloseable {
    protected JavaSparkContext sparkContext;
    private SparkConf sparkConf;

    public LocalSparkExercise() {
        // We need to set the log level to warn in order to filter out uninteresting logging from the cluster
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        // Set up a simple local Spark Cluster
        this.sparkConf = new SparkConf()
                .setAppName("startingSpark") // The name of the cluster
                .setMaster("local[*]");

        // Set up a connection to the Spark cluster configured just above
        this.sparkContext = new JavaSparkContext(sparkConf);
    }

    @Override
    public void close() {
        // Don't forget to close the connection to the cluster after we're done
        this.sparkContext.close();
    }
}
