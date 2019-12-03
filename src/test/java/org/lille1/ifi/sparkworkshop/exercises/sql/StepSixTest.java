package org.lille1.ifi.sparkworkshop.exercises.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

class StepSixTest {

    @Test
    void ex1() {
        try (StepSix stepSix = new StepSix()) {
            final Dataset<Row> rowDataset = stepSix.ex1();
            final List<Row> rows = rowDataset.collectAsList();
            assertThat(rows).hasSize(5);
        }
    }
}
