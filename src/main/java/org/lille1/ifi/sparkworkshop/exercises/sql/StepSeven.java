package org.lille1.ifi.sparkworkshop.exercises.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.lille1.ifi.sparkworkshop.config.SQLSparkExercise;

import java.text.SimpleDateFormat;

/**
 * STEP SEVEN : User defined functions
 *
 * Ce que tu vas apprendre:
 * - Créer de nouvelles fonctions SparkSQL
 * - Les utiliser dans une requète SparkSQL
 */
public class StepSeven extends SQLSparkExercise {
    private final static SimpleDateFormat mmmm = new SimpleDateFormat("MMMM");
    private final static SimpleDateFormat m = new SimpleDateFormat("M");

    public Dataset<Row> ex1() {
        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/data/biglog.txt");
        dataset.createOrReplaceTempView("logging_table");

        // TODO : Enregistrez une fonction dans spark s'appelant monthNum qui va prendre une string en entrée représentant le nom du mois
        // et qui va retourner le numero du mois
        // (hint: pour enregistrer une nouvelle fonction vous aurez besoin d'un nom de fonction, d'une lambda représentant
        // le comportement de la fonction et enfin un type (cf. Datatypes)
        // (hint2: les deux constantes mmmm et m peuvent vous servir à transformer un nom de mois en nombre...)
        spark.udf();

        // TODO: Créer une requète affichant le niveau de log, le mois au format MMMM,
        // et le nombre d'occurence par mois. Triez ensuite les résultats par numéro de mois puis par de niveau de log
        // en utilisant la fonction spark sql que vous venez de créer
        Dataset<Row> results = null;

        results.show(100);
        return results;
    }
}
