package org.lille1.ifi.sparkworkshop.exercises.rdd;

import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

class StepFourTest {

    @Test
    void ex1() {
        try (StepFour stepFour = new StepFour()){
            final List<Tuple2<Long, String>> tuple2s = stepFour.ex1();
            assertThat(tuple2s.get(0)._1).isEqualTo(942);
            assertThat(tuple2s.get(1)._1).isEqualTo(661);
            assertThat(tuple2s.get(2)._1).isEqualTo(376);
            assertThat(tuple2s.get(3)._1).isEqualTo(252);
            assertThat(tuple2s.get(4)._1).isEqualTo(250);
        }
    }
}
