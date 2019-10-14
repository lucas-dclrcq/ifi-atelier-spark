package org.lille1.ifi.sparkworkshop.exercises.rdd;

import org.apache.spark.api.java.JavaRDD;
import org.lille1.ifi.sparkworkshop.config.LocalSparkExercise;

import java.util.List;

/**
 * STEP ONE : Resilient Distributed Datasets (RDD)
 * Dans cette première série d'exercice le but est de découvrir map/reduce et ses possibilités. Pour des raisons pratiques
 * on ne va pas lancer ces exercices sur un "vrai" cluster. Du coup, rien à installer ! Avec une simple dépendance maven
 * il est possible de lancer un cluster local :)
 *
 * Documents utiles:
 * - https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-operations
 * - TODO: api java streams
 *
 * Ce que tu vas apprendre :
 * - Utiliser des fonctions de mapping
 * - Utiliser des fonctions de réduction
 * - Grouper des valeurs grâce aux tuples
 */
public class StepOne extends LocalSparkExercise {
    /**
     * Exercice 1 : Somme de valeurs
     *
     * @param doubles Une liste de doubles dont vous devrez calculer la somme
     * @return La somme des doubles
     */
    public Double ex1(List<Double> doubles) {
        // Transforme la liste de doubles en un "RDD" spark. C'est l'entité de base de spark sur laquelle vous
        // devrez travailler
        final JavaRDD<Double> doubleRDD = this.sparkContext.parallelize(doubles);

        // TODO: Implémenter une fonction de réduction qui prend deux double en entrée et retoure la somme
        // (hint: utiliser une lambda, ou une fonction anonyme)
        final Double sum = null;

        return sum;
    }

    /**
     * Exercice 2: Somme des racines carrées des valeurs
     *
     * @param integers: Une liste de doubles dont vous devrez calculer la somme des racines carrées
     * @return La somme des racines carrées
     */
    public Double ex2(List<Integer> integers) {
        // Transforme la liste de doubles en un "RDD" spark. C'est l'entité de base de spark sur laquelle vous
        // devrez travailler
        final JavaRDD<Integer> rdd = sparkContext.parallelize(integers);

        //TODO: Implémenter une fonction de mapping qui calcule la racine carrée de chaque valeur
        final JavaRDD<Double> squareRootRDD = null;

        // TODO: Implémenter une fonction de réduction qui prend deux double en entrée et retourne la somme de ces doubles
        final Double reduce = null;

        return reduce;
    }

    /**
     * Exercice 3: Compter le nombre de valeurs d'une liste.
     *
     * @param integers
     * @return Le nombre de valeurs
     */
    public Integer ex3(List<Integer> integers) {
        // Transforme la liste de doubles en un "RDD" spark. C'est l'entité de base de spark sur laquelle vous
        // devrez travailler
        final JavaRDD<Integer> rdd = sparkContext.parallelize(integers);

        //TODO: Implémenter une fonction qui prend une valeur et qui retourne... 1
        final JavaRDD<Integer> singleIntegersRDD = null;

        //TODO: Implémenter une fonction de réduction qui prend deux entier en entrée et qui retourne la somme de ces entiers
        final Integer reduce = null;

        // Voila ! vous avez réussi à compter le nombre d'élements dans un rdd réparti sur plusieurs threads :)
        return reduce;
    }
}
