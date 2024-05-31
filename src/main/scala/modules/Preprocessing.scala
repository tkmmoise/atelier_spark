package modules

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.functions._

object Preprocessing {
  def readData(spark: SparkSession, path: String): RDD[String] = {
    spark.sparkContext.textFile(path)
  }

  // Fonction pour déterminer si une ligne est le début d'un nouveau livre
  def isNewBook(line: String): Boolean = {
    line.contains("isbn")
  }

  // Nettoyage et normalisation des textes des livres
  def cleanBooks(booksRDD: RDD[String]): RDD[String] = {
    booksRDD
      .map(book => book.trim) // Suppression des espaces en début et fin de texte
      //.map(book => book.toLowerCase) // Conversion en minuscules
      .map(book => book.replaceAll("\\s+", " ")) // Normalisation des espaces
      .map(book => book.replaceAll("(\\bisbn\\b|\\bcopyright\\b|\\ball rights reserved\\b|\\bprinted in the u\\.s\\.a\\b)", "")) // Suppression des métadonnées
  }
  def cleanBooks(booksDF: DataFrame): DataFrame = {
    booksDF
      .withColumn("books", trim(col("books"))) // Suppression des espaces en début et fin de texte
      //.withColumn("books", lower(col("books"))) // Conversion en minuscules
      .withColumn("books", regexp_replace(col("books"), "\\s+", " ")) // Normalisation des espaces
      .withColumn("books", regexp_replace(col("books"), "(\\bisbn\\b|\\bcopyright\\b|\\ball rights reserved\\b|\\bprinted in the u\\.s\\.a\\b)", "")) // Suppression des métadonnées
  }


  // Groupement des lignes pour former des livres complets
  def groupLinesToBooks(linesRDD: RDD[String]): RDD[String] = {
    linesRDD.mapPartitions { iter =>
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
  }

  // Conversion RDD to Dataframe
  def convertToDF(spark: SparkSession, rdd: RDD[String]): DataFrame = {
    val schema = new StructType().add("books", StringType)
    spark.createDataFrame(rdd.map(Row(_)), schema)
  }
}
