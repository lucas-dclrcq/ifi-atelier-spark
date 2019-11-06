package rdd;

import org.junit.jupiter.api.Test;
import org.lille1.ifi.sparkworkshop.Baby;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class BabyTest {
    @Test
    void ex1_given_a_list_of_doubles_should_calculate_the_total_sum() {
        final List<Double> doubles = Arrays.asList(
                35.5,
                12.49943,
                90.32,
                20.32
        );

        try (Baby baby = new Baby()) {
            final Double result = baby.ex1_calculate_the_total_sum_of_a_list_of_doubles(doubles);
            assertThat(result)
                    .isEqualTo(158.63942999999998);
        }
    }

    @Test
    void ex2() {
        final List<Integer> integers = Arrays.asList(
                35,
                12,
                90,
                20
        );

        try (Baby baby = new Baby()) {
            final Double result = baby.ex2(integers);
            assertThat(result)
                    .isEqualTo(23.339150333742086);
        }
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

        try (Baby baby = new Baby()) {
            final Integer result = baby.ex3(integers);
            assertThat(result)
                    .isEqualTo(4);
        }
    }
}
