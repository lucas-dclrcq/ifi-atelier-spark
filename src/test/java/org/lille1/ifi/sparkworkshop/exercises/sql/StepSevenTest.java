package org.lille1.ifi.sparkworkshop.exercises.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

class StepSevenTest {

    @Test
    void ex1() {
        try (StepSeven stepSeven = new StepSeven()) {
            final Dataset<Row> rowDataset = stepSeven.ex1();
            final List<Row> rows = rowDataset.collectAsList();
            assertThat(rows).hasSize(60);
        }
    }
}
