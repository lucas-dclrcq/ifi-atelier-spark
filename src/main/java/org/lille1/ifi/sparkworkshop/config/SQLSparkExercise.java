package org.lille1.ifi.sparkworkshop.config;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

public class SQLSparkExercise implements AutoCloseable {
    protected SparkSession spark;

    public SQLSparkExercise() {
        // We need to set the log level to warn in order to filter out uninteresting logging from the cluster
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        // Set up a simple local Spark Cluster with support for SparkSQL
        this.spark = SparkSession.builder().appName("testingSql").master("local[*]")
                .config("spark.sql.warehouse.dir","file:///tmp/")
                .getOrCreate();
    }

    @Override
    public void close() {
        // Don't forget to close the connection to the cluster after we're done
        this.spark.close();
    }
}
