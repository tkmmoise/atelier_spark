import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}

import vegas._
import vegas.render.WindowRenderer._

object SimpleApp extends App {

    // Configuration de Spark
    val spark = SparkSession.builder()
      .appName("Lecture de données de livres")
      .master("local[*]")
      .getOrCreate()

    // Chemins des fichiers de données
    val filePath1 = "./data/bookcorpus/books_large_p1.txt"
    val filePath2 = "./data/bookcorpus/books_large_p2.txt"


    // Lecture des données des deux fichiers
    val dataRDD1 = spark.sparkContext.textFile(filePath1)
    val dataRDD2 = spark.sparkContext.textFile(filePath2)

    // Combinaison des DataFrames
    val linesRDD = dataRDD1.union(dataRDD2)

    // Fonction pour déterminer si une ligne est le début d'un nouveau livre
    def isNewBook(line: String): Boolean = {
        line.contains("isbn")
        // line.startsWith("isbn") || line.startsWith("chapter")
    }

    // Groupement des lignes pour former des livres complets
    def groupLinesToBooks(linesDF: RDD[String]): DataFrame = {
        val booksRDD = linesRDD.mapPartitions { iter =>
            var currentBook = List[String]()
            iter.flatMap { line =>
                if (isNewBook(line)) {
                    val book = currentBook.reverse.mkString("\n")
                    currentBook = List(line)
                    if (book.nonEmpty) Some(book) else None
                } else {
                    currentBook = line :: currentBook
                    None
                }
            } ++ (if (currentBook.nonEmpty) Some(currentBook.reverse.mkString("\n")) else None)
        }
        val schema = new StructType().add("livres", StringType)
        spark.createDataFrame(booksRDD.map(Row(_)), schema)
    }

    // Fonction pour compter le nombre de mots dans une chaîne
    def countWords(text: String): Int = text.split("\\s+").length

    // Ajout de la colonne "nombre de mots" au DataFrame
    def addWordCountColumn(booksDF: DataFrame): DataFrame = {
        val countWordsUDF = udf((text: String) => countWords(text))
        booksDF.withColumn("nombre de mots", countWordsUDF(col("livres")))
    }

    // Traitement des données
    var booksDF = groupLinesToBooks(linesRDD)
    booksDF = addWordCountColumn(booksDF)

    // Affichage du DataFrame résultant
    booksDF.show()

    // ###########################################VISUALISATION################################################
    // Histogramme pour la distribution du nombre de mots par livre
    // val wordCountHist =

    // print("Num of lines : ", booksDF.count())
    spark.stop()
}
