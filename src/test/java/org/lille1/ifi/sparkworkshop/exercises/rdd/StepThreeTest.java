package org.lille1.ifi.sparkworkshop.exercises.rdd;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

class StepThreeTest {

    @Test
    void ex1_a() {
        try (StepThree stepThree = new StepThree()) {
            final List<String> result = stepThree.ex1("src/main/resources/data/beaudelaire_1.txt");
            assertThat(result).allMatch(s -> !s.contains(" "));
            assertThat(result).hasSize(113);
        }
    }

    @Test
    void ex1_b() {
        try (StepThree stepThree = new StepThree()) {
            final List<String> result = stepThree.ex1("src/main/resources/data/beaudelaire_2.txt");
            assertThat(result).allMatch(s -> !s.contains(" "));
            assertThat(result).hasSize(141);
        }
    }

    @Test
    void ex2_a() {
        try (StepThree stepThree = new StepThree()) {
            final List<String> result = stepThree.ex2("src/main/resources/data/beaudelaire_1.txt", "e");
            assertThat(result).allMatch(s -> !s.startsWith("e"));
            assertThat(result).hasSize(110);
        }
    }

    @Test
    void ex2_b() {
        try (StepThree stepThree = new StepThree()) {
            final List<String> result = stepThree.ex2("src/main/resources/data/beaudelaire_2.txt", "e");
            assertThat(result).allMatch(s -> !s.startsWith("e"));
            assertThat(result).hasSize(129);
        }
    }
}
