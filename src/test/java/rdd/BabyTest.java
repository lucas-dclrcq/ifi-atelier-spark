package rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * In this first series of exercises the goal is to discover the basics of map/reduce functions
 * For now we'll run the calculations against a local Spark cluster. You don't need to install anything ! By using a simple maven dependency we're able to launch a local cluster :)
 *
 * When you're finished you'll have learned how to :
 * - Use mapping functions
 * - Use reduce functions
 * - Use Tuples to group values
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BabyTest {
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
    void ex1_given_a_list_of_doubles_should_calculate_the_total_sum() {
        // Initialize a simple list of doubles, which we'll use in the rest of the exercise
        final List<Double> doubles = Arrays.asList(
                35.5,
                12.49943,
                90.32,
                20.32
        );

        // Transforms the list from above into a Spark RDD
        final JavaRDD<Double> rdd = sparkContext.parallelize(doubles);

        // TODO: Implement a reduce fonction that'll take two doubles and return the sum
        //  (hint: use a lambda, or a method reference)
        final Double reduce = rdd.reduce(Double::sum);

        assertThat(reduce).isEqualTo(158.63943);
    }

    @Test
    void ex2() {
        // Initialize a simple list of integers, which we'll use in the rest of the exercise
        final List<Integer> integers = Arrays.asList(
                35,
                12,
                90,
                20
        );

        // Transforms the list from above into a Spark RDD
        final JavaRDD<Integer> rdd = sparkContext.parallelize(integers);

        //TODO: Implement a mapping function that'll take an integer and return its square root
        final JavaRDD<Double> squareRootRDD = rdd.map(Math::sqrt);

        //TODO: Implement a reduce fonction that'll take two doubles and return the sum
        final Double reduce = squareRootRDD.reduce(Double::sum);

        assertThat(reduce).isEqualTo(23.339150333742086);
    }

    @Test
    void ex3() {
        // Initialize a simple list of integers, which we'll use in the rest of the exercise
        final List<Integer> integers = Arrays.asList(
                35,
                12,
                90,
                20
        );

        // Transforms the list from above into a Spark RDD
        final JavaRDD<Integer> rdd = sparkContext.parallelize(integers);

        //TODO: Implement a mapping function that'll take an integer and return its square root
        final JavaRDD<Double> squareRootRDD = rdd.map(Math::sqrt);

        //TODO: Implement a function that take a value.... and returns 1
        final JavaRDD<Integer> singleIntegersRDD = squareRootRDD.map(value -> 1);

        //TODO: Implement a reduce fonction that'll take two integers and return the sum
        final Integer reduce = singleIntegersRDD.reduce(Integer::sum);

        // Noice! You implemented a spark function that count your square roots !
        assertThat(reduce).isEqualTo(23.339150333742086);
    }

    @Test
    void ex4() {
        // Initialize a simple list of integers, which we'll use in the rest of the exercise
        final List<Integer> integers = Arrays.asList(
                35,
                12,
                90,
                20
        );

        // Transforms the list from above into a Spark RDD
        final JavaRDD<Integer> originalIntegers = sparkContext.parallelize(integers);

        //TODO: Implement a mapping function that'll take an integer and return its square root
        final JavaRDD<Tuple2<Integer, Double>> squareRoots = originalIntegers.map(value -> new Tuple2<>(value, Math.sqrt(value)));

    }
}
