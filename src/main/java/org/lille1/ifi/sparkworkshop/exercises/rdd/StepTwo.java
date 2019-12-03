package org.lille1.ifi.sparkworkshop.exercises.rdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.lille1.ifi.sparkworkshop.config.LocalSparkExercise;
import scala.Tuple2;

import java.util.List;

/**
 * STEP 2 : Pair RDDs
 *
 * Ce que tu vas apprendre:
 * - Grouper des tuples par leur clé
 * - Réduire des tuples par leur clé
 * - Utiliser l'API fluent de RDD
 */
public class StepTwo extends LocalSparkExercise {
    /**
     * Exercice 1 : Aggrégation par clé. Dans cet exercice nous allons prendre une liste de string représentant des logs et nous allons
     * les grouper par niveau de log (WARN, ERROR, INFO, FATAL, etc...)
     *
     * Attention : grouper par clé peut entrainer des problèmes de performance sur des gros volumes de données, nous
     * verrons une meilleure facon de faire plus tard.
     *
     * @param logs
     */
    public Long ex1(List<String> logs, String logLevel) {
        final JavaRDD<String> originalLogMessages = sparkContext.parallelize(logs);

        // TODO: En utilisant la fonction rdd.maptoPair() vous devez mapper une string vers un tuple (niveauDeLog, date)
        // (hint: un tuple de deux valeurs s'initialise comme ça : new Tuple2<>(premiereValeur, deuxiemeValeur))
        // (hint2: pour voir à quoi ressemble les logs allez voir le fichier src/main/resources/data/biglog.txt)
        final JavaPairRDD<String, String> pairRDDs = null;

        // TODO: Trouver comment grouper les tuples par clef
        // (hint: Do you know javadoc ?)
        final JavaPairRDD<String, Iterable<String>> stringIterableJavaPairRDD = null;

        // Ne pas modifier
        return stringIterableJavaPairRDD
                .collectAsMap()
                .get(logLevel)
                .spliterator()
                .getExactSizeIfKnown();
    }

    /**
     * Exercice 2 : Reduction par clé. Ici nous allons essayer de compter le nombre d'occurence pour chaque niveau de log
     * (WARN, ERROR, INFO, FATAL, etc...)
     *
     * @param logs
     */
    public Long ex2(List<String> logs, String logLevel) {
        final JavaRDD<String> originalLogMessages = sparkContext.parallelize(logs);

        // TODO: Ici vous devrez, pour chaque ligne de log, créer un tuple du type (LOGLEVEL, 1L) en utilisant la
        // fonction mapToPair
        final JavaPairRDD<String, Long> pairRDDs = null;

        // TODO: Ici vous devrez utiliser la fonction rdd.reduceByKey() pour trouver le total d'entrée pour chaque
        // niveau de log
        // (hint: reprendre le principe du Step1)
        final JavaPairRDD<String, Long> sumsRDDS = null;

        // NE pas modifier
        return sumsRDDS.collectAsMap()
                .get(logLevel);
    }


    /**
     * Exercise 3: The Fluent Way.
     *
     * @param logs
     */
    public Long ex3(List<String> logs, String logLevel) {
        final JavaRDD<String> parallelize = sparkContext.parallelize(logs);

        // TODO: ici vous devez faire exactement la même chose que précédemment mais en ne déclarant pas de nouvelles variables
        // (hint: vous devez utiliser la forme "fluent" un exemple :
        // variable.faireUnTruc()
        //         .faireUnAutreTruc()
        //         .finalementFaireUndernierTruc()
        final JavaPairRDD<String, Long> stringLongJavaPairRDD = null;

        // Ne pas modifier
        return stringLongJavaPairRDD
                .collectAsMap()
                .get(logLevel);
    }
}
