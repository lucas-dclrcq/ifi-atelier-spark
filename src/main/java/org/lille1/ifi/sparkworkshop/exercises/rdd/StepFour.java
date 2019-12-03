package org.lille1.ifi.sparkworkshop.exercises.rdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.lille1.ifi.sparkworkshop.config.CommonWords;
import org.lille1.ifi.sparkworkshop.config.LocalSparkExercise;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * STEP FOUR: Rick and Morty Word Ranking.
 *
 * Ici on va parser tous les sous-titres de la série Rick and Morty et trouver les plus utilisés dans cette série.
 *
 * Ce que tu vas apprendre:
 * - Bilan de tout ce que l'on a vu jusqu'a présent !
 */
public class StepFour extends LocalSparkExercise {

    public List<Tuple2<Long, String>> ex1() {
        final JavaRDD<String> initialFile = sparkContext.textFile("src/main/resources/data/rickandmorty/*.srt");

        // TODO: Supprimer les caractères non alphabétiques (majuscule ET minuscules)
        // (hint: utiliser une fonction de mapping)
        // (hint: Do you know REGEX ?)
        final JavaRDD<String> onlyLetters = null;

        // TODO: Créer une fonction de mapping qui transforme chaque string en sa version lowercase!
        final JavaRDD<String> lowercase = null;

        // TODO: Supprimer les espaces en début/fin de chaque ligne
        // (hint: utiliser une fonction de mapping)
        final JavaRDD<String> trimmed = null;

        // TODO: Supprimer les lignes blanches en implementant une fonction de filtre
        // (hint: utiliser une fonction de filtre)
        final JavaRDD<String> noBlankLines = null;

        // TODO: Extraire les mots de chaque ligne en utilisant la fonction flatMap
        final JavaRDD<String> words = null;

        // TODO: Supprimer les mots communs
        // (hint: utiliser la classe utils CommonWords pour filter les mots communs)
        final JavaRDD<String> uncommonWords = null;

        // TODO: En reprenant le principe du Step3, compter le nombre d'occurence de chaque mot
        final JavaPairRDD<String, Long> stringLongJavaPairRDD = null;
        final JavaPairRDD<String, Long> wordCounts = null;

        // TODO: Trier les mots par nombre d'occurence
        //(hint: utiliser la fonction sortByKey )
        final JavaPairRDD<Long, String> switched = null;
        final JavaPairRDD<Long, String> sortedWords = null;

        // Ne pas modifier
        final List<Tuple2<Long, String>> leaderboard = sortedWords.take(50);
        leaderboard
                .forEach(entry -> System.out.println(entry._1 + " => " + entry._2));

        return leaderboard;
    }
}
