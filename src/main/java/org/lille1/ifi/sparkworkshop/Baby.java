package org.lille1.ifi.sparkworkshop;

import org.apache.spark.api.java.JavaRDD;

import java.util.List;

/**
 * In this first series of exercises the goal is to discover the basics of map/reduce functions
 * For now we'll run the calculations against a local Spark cluster. You don't need to install anything ! By using a simple maven dependency we're able to launch a local cluster :)
 *
 * When you're finished you'll have learned how to :
 * - Use mapping functions
 * - Use reduce functions
 * - Use Tuples to group values
 */
public class Baby extends LocalSparkExercise {
    /**
     * Exercice 2 :
     * @param doubles
     * @return
     */
    public Double ex1_calculate_the_total_sum_of_a_list_of_doubles(List<Double> doubles) {
        // Transforms the list from above into a Spark RDD
        final JavaRDD<Double> soubleSparkRDD = this.sparkContext.parallelize(doubles);

        // TODO: Implement a reduce fonction that'll take two doubles and return the sum
        //  (hint: use a lambda, or a method reference)
        final Double sum = soubleSparkRDD.reduce(Double::sum);

        return sum;
    }

    /**
     * Exercice 2:
     *
     * @param integers
     * @return
     */
    public Double ex2(List<Integer> integers) {
        // Transforms the list from above into a Spark RDD
        final JavaRDD<Integer> rdd = sparkContext.parallelize(integers);

        //TODO: Implement a mapping function that'll take an integer and return its square root
        final JavaRDD<Double> squareRootRDD = rdd.map(Math::sqrt);

        //TODO: Implement a reduce fonction that'll take two doubles and return the sum
        final Double reduce = squareRootRDD.reduce(Double::sum);

        return reduce;
    }

    /**
     * Exercice 3:
     *
     * @param integers
     * @return
     */
    public Integer ex3(List<Integer> integers) {
        // Transforms the list from above into a Spark RDD
        final JavaRDD<Integer> rdd = sparkContext.parallelize(integers);

        //TODO: Implement a mapping function that'll take an integer and return its square root
        final JavaRDD<Double> squareRootRDD = rdd.map(Math::sqrt);

        //TODO: Implement a function that take a value.... and returns 1
        final JavaRDD<Integer> singleIntegersRDD = squareRootRDD.map(value -> 1);

        //TODO: Implement a reduce fonction that'll take two integers and return the sum
        final Integer reduce = singleIntegersRDD.reduce(Integer::sum);

        // Noice! You implemented a spark function that count your square roots !
        return reduce;
    }
}
