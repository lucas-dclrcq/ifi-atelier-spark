package org.lille1.ifi.sparkworkshop.exercises.sql;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.lille1.ifi.sparkworkshop.config.SQLSparkExercise;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * Step Six : Pivot tables
 *
 * Ressources:
 * - https://sparkbyexamples.com/spark/how-to-pivot-table-and-unpivot-a-spark-dataframe/
 *
 * Ce que tu vas apprendre:
 * - Pivoter les lignes en colonnes pour utiliser différemment les données.
 */
public class StepSix extends SQLSparkExercise {
    public Dataset<Row>  ex1() {
        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/data/biglog.txt");

        // TODO: comme vu précédemment écrivez un select qui affichera chaque ligne sous la forme
        // SELECT level, month, monthnum FROM logging_level
        dataset = null;

        // Ne pas modifier
        System.out.println("Données dans leur forme originelle :");
        dataset.show(100);

        // TODO: créer une liste d'objets contenant les noms des mois de l'année (en angliche)
        List<Object> columns = null;

        // TODO: utiliser les fonctions pivot et count pour transformer votre dataset en affichant le nombre de log par mois sur chaque colonne
        dataset = null;

        // Ne pas modifier
        System.out.println("Données pivotées :");
        dataset.show(100);
        return dataset;
    }
}
