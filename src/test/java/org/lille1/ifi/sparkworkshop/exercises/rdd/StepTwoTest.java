package org.lille1.ifi.sparkworkshop.exercises.rdd;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

class StepTwoTest {

    @Test
    void given_list_of_logs_and_level_should_return_number_of_occurence_for_level() {
        try (StepTwo stepTwo = new StepTwo()) {
            final List<String> logs = Arrays.asList(
                    "WARN,2016-4-4 14:55:51",
                    "INFO,2014-9-25 15:03:32",
                    "DEBUG,2015-2-2 07:34:59",
                    "FATAL,2012-11-5 23:47:57",
                    "INFO,2017-12-16 16:39:35",
                    "DEBUG,2015-11-27 13:16:45",
                    "DEBUG,2013-3-26 13:02:03",
                    "INFO,2013-2-5 03:38:08",
                    "INFO,2017-7-16 00:22:06",
                    "DEBUG,2016-10-19 17:06:28",
                    "ERROR,2015-10-26 00:04:01",
                    "INFO,2013-4-1 18:32:22",
                    "INFO,2017-2-28 14:57:10",
                    "DEBUG,2017-7-15 11:22:54",
                    "INFO,2016-10-25 05:23:03",
                    "DEBUG,2014-7-26 22:41:13"
            );
            final Long result = stepTwo.ex1(logs, "INFO");
            assertThat(result).isEqualTo(7);
        }
    }

    @Test
    void ex2() {
        try (StepTwo stepTwo = new StepTwo()) {
            final List<String> logs = Arrays.asList(
                    "WARN,2016-4-4 14:55:51",
                    "INFO,2014-9-25 15:03:32",
                    "DEBUG,2015-2-2 07:34:59",
                    "FATAL,2012-11-5 23:47:57",
                    "INFO,2017-12-16 16:39:35",
                    "DEBUG,2015-11-27 13:16:45",
                    "DEBUG,2013-3-26 13:02:03",
                    "INFO,2013-2-5 03:38:08",
                    "INFO,2017-7-16 00:22:06",
                    "DEBUG,2016-10-19 17:06:28",
                    "ERROR,2015-10-26 00:04:01",
                    "INFO,2013-4-1 18:32:22",
                    "INFO,2017-2-28 14:57:10",
                    "DEBUG,2017-7-15 11:22:54",
                    "INFO,2016-10-25 05:23:03",
                    "DEBUG,2014-7-26 22:41:13"
            );
            final Long result = stepTwo.ex2(logs, "DEBUG");
            assertThat(result).isEqualTo(6);
        }
    }

    @Test
    void ex3() {
        try (StepTwo stepTwo = new StepTwo()) {
            final List<String> logs = Arrays.asList(
                    "WARN,2016-4-4 14:55:51",
                    "INFO,2014-9-25 15:03:32",
                    "DEBUG,2015-2-2 07:34:59",
                    "FATAL,2012-11-5 23:47:57",
                    "INFO,2017-12-16 16:39:35",
                    "DEBUG,2015-11-27 13:16:45",
                    "DEBUG,2013-3-26 13:02:03",
                    "INFO,2013-2-5 03:38:08",
                    "INFO,2017-7-16 00:22:06",
                    "DEBUG,2016-10-19 17:06:28",
                    "ERROR,2015-10-26 00:04:01",
                    "INFO,2013-4-1 18:32:22",
                    "INFO,2017-2-28 14:57:10",
                    "DEBUG,2017-7-15 11:22:54",
                    "INFO,2016-10-25 05:23:03",
                    "DEBUG,2014-7-26 22:41:13"
            );
            final Long result = stepTwo.ex3(logs, "ERROR");
            assertThat(result).isEqualTo(1);
        }
    }
}
