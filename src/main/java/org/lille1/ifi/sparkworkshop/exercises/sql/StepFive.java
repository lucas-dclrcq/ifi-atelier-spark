package org.lille1.ifi.sparkworkshop.exercises.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.lille1.ifi.sparkworkshop.config.SQLSparkExercise;

import static org.apache.spark.sql.functions.*;


/**
 * STEP 5 : Datasets.
 *
 * Ce que tu vas apprendre :
 * - Manipuler une base de données relationnelle grâce à SparkSQL
 */
public class StepFive extends SQLSparkExercise {

    public Dataset<Row> ex1() {
        // Ici on charge le gros fichier de log dans spark
        Dataset<Row> dataset = spark
                .read()
                .option("header", true)
                .csv("src/main/resources/data/biglog.txt");

        // On crée une vue correspondant au fichier de logs
        // Les requètes Spark SQL utiliseront cette table, par exemple un simple select :
        // SELECT * FROM logging_table
        dataset.createOrReplaceTempView("logging_table");

        // TODO: Implémenter une requète sql qui va afficher le niveau de log, le mois au format JANV et le chiffre 1.
        // Exemple :  ERROR, JANV, 1
        Dataset<Row> results = null;

        // Ne pas modifier
        results.show(100);
        return results;
    }

    public Dataset<Row> ex2() {
        // Ici on charge le gros fichier de log dans spark
        Dataset<Row> dataset = spark
                .read()
                .option("header", true)
                .csv("src/main/resources/data/biglog.txt");

        // On crée une vue correspondant au fichier de logs
        // Les requètes Spark SQL utiliseront cette table, par exemple un simple select :
        // SELECT * FROM logging_table
        dataset.createOrReplaceTempView("logging_table");

        // TODO: Réutiliser votre requète SQL de l'exercice 1, en la modifiant pour grouper les lignes par niveau de log puis par mois et pour quelle affiche un troisième paramètre correspondant au total de lignes de log
        // (hint: utiliser la fonction count() pour compter le nombre de lignes)
        // (hint2: utiliser un group by)
        Dataset<Row> results = null;

        // Ne pas modifier
        results.show(100);
        return results;
    }

    public Dataset<Row> ex3() {
        // Ici on charge le gros fichier de log dans spark
        Dataset<Row> dataset = spark
                .read()
                .option("header", true)
                .csv("src/main/resources/data/biglog.txt");

        // On crée une vue correspondant au fichier de logs
        // Les requètes Spark SQL utiliseront cette table, par exemple un simple select :
        // SELECT * FROM logging_table
        dataset.createOrReplaceTempView("logging_table");

        // TODO: Réutiliser votre requète SQL de l'exercice 2, en la modifiant pour trier les lignes par niveau de log
        // puis par mois
        // (hint: utiliser un order by)
        // (hint2: pour pouvoir trier par mois on va convertir chaque mois en int en utilisant la fonction : cast(first(date_format(datetime,'M'))
        Dataset<Row> results = null;

        // Ne pas modifier
        results.show(100);
        return results;
    }

    /**
     * Exercice 4: Reproduire exactement ce que vous avez fait à l'exercice 3 mais en utilisant des fonctions Java plutot
     * qu'une requète sql
     *
     * Hint: chaque partie de la requète SQL correspond à une fonction java que vous pourrez trouver ici :
     * https://spark.apache.org/docs/2.0.2/api/java/org/apache/spark/sql/functions.html
     *
     * Exemple : "SELECT level FROM logging_table" devient : dataset.select(col("level"));
     *
     * @return
     */
    public Dataset<Row> ex4() {
        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/data/biglog.txt");
        dataset.createOrReplaceTempView("logging_table");

        // TODO: Implémenter le select au format 'level, mois, numéro de mois')
        dataset = null;

        // TODO: Implémenter le group by par niveau de log puis par mois, puis par numéro de mois
        dataset = null;

        // TODO: Implémenter le order by par numéro de mois puis par niveau de log
        dataset = null;

        // TODO: Supprimer la colonne correspondant au numéro du mois
        // (hint: la fonction drop permet de supprimer une colonne lorsque elle devient inutile)
        dataset = null;

        // Ne pas modifier
        dataset.show(100);
        return dataset;
    }
}
