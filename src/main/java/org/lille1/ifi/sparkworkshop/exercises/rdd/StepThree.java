package org.lille1.ifi.sparkworkshop.exercises.rdd;

import org.apache.spark.api.java.JavaRDD;
import org.lille1.ifi.sparkworkshop.config.LocalSparkExercise;

import java.util.Arrays;
import java.util.List;

/**
 * STEP THREE : Flat Maps and Filters
 *
 * Ressources:
 * - https://www.baeldung.com/java-difference-map-and-flatmap
 *
 * Ce que tu vas apprendre :
 * - Utiliser les flat maps
 * - Utiliser les filtres
 */
public class StepThree extends LocalSparkExercise {

    /**
     * Exercise 1: Flat Maps.
     *
     *
     * @param poemPath Le chemin du fichier du poème
     * @return Les mots du poème
     */
    public List<String> ex1(String poemPath) {
        // La fonction textFile de Spark nous permer de charger un fichier texte facilement et de le transformer
        // en RDD
        final JavaRDD<String> sentences = sparkContext.textFile(poemPath);

        // TODO: Utiliser flat map pour récupérer la liste de tous les mots du poeme
        // (hint: duh)
        final JavaRDD<String> words = null;

        return words.collect();
    }

    /**
     * Exercice 2: Filters
     * @param poemPath Le chemin du fichier du poème
     * @param forbiddenWordStart La lettre interdite pour les début de mots
     * @return Les mots du poeme sans les mots commencant par
     */
    public List<String> ex2(String poemPath, String forbiddenWordStart) {
        final JavaRDD<String> sentences = sparkContext.textFile(poemPath);

        // TODO: Utiliser flat map pour récupérer la liste de tous les mots du poeme
        // (hint: the truth lies between the words)
        final JavaRDD<String> words = null;

        // TODO: Utiliser filter pour retirer tous les mots commencant par e
        // (hint: duh)
        final JavaRDD<String> filteredWords = null;

        return filteredWords.collect();
    }
}
