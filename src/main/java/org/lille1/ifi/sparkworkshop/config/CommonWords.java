package org.lille1.ifi.sparkworkshop.config;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Set;
import java.util.stream.Collectors;

public class CommonWords {
    private static Set<String> commonWords;

    static {
        final InputStream is = CommonWords.class.getClassLoader().getResourceAsStream("data/commonwords.txt");
        final BufferedReader br = new BufferedReader(new InputStreamReader(is));
        commonWords = br.lines().collect(Collectors.toSet());
    }

    public static boolean isCommon(String word) {
        return commonWords.contains(word);
    }

    public static boolean isNotCommon(String word) {
        return !isCommon(word);
    }
}
