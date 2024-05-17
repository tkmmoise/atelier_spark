import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline

object ExtractBooks extends App {

  // Configuration de Spark avec augmentation de la taille du tas JVM
  val spark = SparkSession.builder()
    .appName("Lecture de données de livres")
    .master("local[*]")
    .config("spark.driver.memory", "4g") // Augmentez la valeur selon vos besoins
    .config("spark.executor.memory", "4g") // Augmentez la valeur selon vos besoins
    .getOrCreate()

  // Chemins des fichiers de données
  val filePath1 = "/home/matthias/Desktop/EPSI/SPARK-SCALA/bookcorpus/books_large_p1.txt" // Chemin du premier fichier
  val filePath2 = "/home/matthias/Desktop/EPSI/SPARK-SCALA/bookcorpus/books_large_p2.txt" // Chemin du deuxième fichier

  // Lecture des données des deux fichiers
  val dataRDD1 = spark.sparkContext.textFile(filePath1)
  val dataRDD2 = spark.sparkContext.textFile(filePath2)

  // Combinaison des DataFrames
  val linesRDD = dataRDD1.union(dataRDD2)

  // Fonction pour déterminer si une ligne est le début d'un nouveau livre
  def isNewBook(line: String): Boolean = {
    line.contains("isbn")
  }

  // Groupement des lignes pour former des livres complets
  def groupLinesToBooks(linesRDD: RDD[String]): DataFrame = {
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
    val schema = new StructType().add("text", StringType)
    spark.createDataFrame(booksRDD.map(Row(_)), schema)
  }

  // Fonction pour compter le nombre de mots dans une chaîne
  def countWords(text: String): Int = text.split("\\s+").length

  // Ajout de la colonne "nombre de mots" au DataFrame
  def addWordCountColumn(booksDF: DataFrame): DataFrame = {
    val countWordsUDF = udf((text: String) => countWords(text))
    booksDF.withColumn("nombre de mots", countWordsUDF(col("text")))
  }

  // Traitement des données
  var booksDF = groupLinesToBooks(linesRDD)
  booksDF = addWordCountColumn(booksDF)

  booksDF.show()

  spark.stop()
}

