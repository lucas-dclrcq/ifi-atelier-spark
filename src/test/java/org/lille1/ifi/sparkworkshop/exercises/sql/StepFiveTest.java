package org.lille1.ifi.sparkworkshop.exercises.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

class StepFiveTest {
    @Test
    void ex1() {
        try (StepFive stepFive = new StepFive()) {
            final Dataset<Row> rowDataset = stepFive.ex1();
            final List<Row> rows = rowDataset.collectAsList();
            assertThat(rows).hasSize(1000000);
        }
    }

    @Test
    void ex2() {
        try (StepFive stepFive = new StepFive()) {
            final Dataset<Row> rowDataset = stepFive.ex2();
            final List<Row> rows = rowDataset.collectAsList();
            assertThat(rows).hasSize(60);
        }
    }

    @Test
    void ex3() {
        try (StepFive stepFive = new StepFive()) {
            final Dataset<Row> rowDataset = stepFive.ex3();
            final List<Row> rows = rowDataset.collectAsList();
            assertThat(rows).hasSize(60);
        }
    }

    @Test
    void ex4() {
        try (StepFive stepFive = new StepFive()) {
            final Dataset<Row> rowDataset = stepFive.ex4();
            final List<Row> rows = rowDataset.collectAsList();
            assertThat(rows).hasSize(60);
        }
    }
}
