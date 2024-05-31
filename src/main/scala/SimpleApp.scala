import org.apache.spark.sql.SparkSession
import modules.{Analysis, Preprocessing, Visualization}
import org.apache.spark.sql.functions.{col, size, split}

object SimpleApp extends App {

    // Configuration de Spark
    val spark = SparkSession.builder()
      .appName("Lecture de données de livres")
      .master("local[*]")
      .getOrCreate()

    // Chemins des fichiers de données
    val filePath1 = "./data/bookcorpus/books_large_p1.txt"
    val filePath2 = "./data/bookcorpus/books_large_p2.txt"

    /* ##########################################################
    ######################## Preprocessing ######################
    ############################################################# */
    // Lecture des données des deux fichiers
    val dataRDD1 = Preprocessing.readData(spark, filePath1)
    val dataRDD2 = Preprocessing.readData(spark, filePath2)

    // Combinaison des RDD
    val linesRDD = dataRDD1.union(dataRDD2)

    // Découpage en livres
    var booksRDD = Preprocessing.groupLinesToBooks(linesRDD)

    // RDD to Dataframe
    var booksDF = Preprocessing.convertToDF(spark, booksRDD)

    // Échantillonnage pour optimiser le developpement
    val sampleFraction = 0.01 // Utiliser une fraction de 1% des données
    booksDF = booksDF.sample(withReplacement = false, fraction = sampleFraction, seed = 42)

    // Nettoyage du Dataframe
    booksDF = Preprocessing.cleanBooks(booksDF)

    /* ##########################################################
    ######################## Analysis ######################
    ############################################################# */
    Analysis.printStatistics(booksDF)


    /* ##########################################################
    ######################## Visualization ######################
    ############################################################# */
    // Calcul du nombre de mots par livre
    val wordsPerBookDF = booksDF.withColumn("num_words", size(split(col("books"), "\\s+")))

    Visualization.plotHistogram(wordsPerBookDF, spark)
    Visualization.plotTop10Books(wordsPerBookDF, spark)



    spark.stop()
}
