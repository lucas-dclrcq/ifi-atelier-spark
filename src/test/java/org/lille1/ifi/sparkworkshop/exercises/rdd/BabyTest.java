package org.lille1.ifi.sparkworkshop.exercises.rdd;

import org.apache.commons.math3.util.Precision;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class BabyTest {
    @Test
    void given_a_list_of_doubles_should_calculate_the_total_sum() {
        final List<Double> doubles = Arrays.asList(
                35.5,
                12.49943,
                90.32,
                20.32
        );

        try (Baby baby = new Baby()) {
            final Double result = Precision.round(baby.ex1(doubles), 12);
            assertThat(result)
                    .isEqualTo(158.639429999999);
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
            final Double result = Precision.round(baby.ex2(integers), 12);
            assertThat(result)
                    .isEqualTo(23.339150333742);
        }
    }

    @Test
    void ex3() {
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
