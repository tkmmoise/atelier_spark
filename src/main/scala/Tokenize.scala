import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.StructType
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline

import com.johnsnowlabs.nlp.Finisher
import org.apache.spark.ml.Pipeline

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.StructType
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.feature.{CountVectorizer, RegexTokenizer, StopWordsRemover}

object Tokenize extends App {

  // Fonction pour imprimer en vert
  def printGreen(text: String): Unit = println("\u001b[32m" + text + "\u001b[0m")

  val filePath1 = "/home/matthias/Desktop/EPSI/SPARK-SCALA/bookcorpus/books_large_p1.txt" // Chemin du premier fichier
  val filePath2 = "/home/matthias/Desktop/EPSI/SPARK-SCALA/bookcorpus/books_large_p2.txt" // Chemin du deuxième fichier
  val delimiter = "isbn-13"

  val spark = SparkSession.builder()
    .appName("Lecture de données de livres")
    .master("local[*]") // ou tout autre mode de déploiement
    .getOrCreate()

  // Fonction pour diviser le contenu en utilisant le délimiteur "isbn-13"
  def splitContent(content: String): Array[String] = content.split(delimiter)

  // Lecture des données du premier fichier et division
  val dataRDD1 = spark.sparkContext.textFile(filePath1).flatMap(splitContent)

  // Lecture des données du deuxième fichier et division
  val dataRDD2 = spark.sparkContext.textFile(filePath2).flatMap(splitContent)

  // Fusion des données des deux fichiers en une seule RDD
  val combinedDataRDD = dataRDD1.union(dataRDD2)

  // Création du DataFrame avec une seule colonne
  val schema = new StructType().add("text", "string")
  val rowRDD = combinedDataRDD.map(line => Row(line.trim))
  var df = spark.createDataFrame(rowRDD, schema)

  // Fonction pour compter le nombre de mots dans une chaîne
  def countWords(text: String): Int = text.split("\\s+").length

  // Ajout de la colonne "nombre de mots" au DataFrame
  val countWordsUDF = udf((text: String) => countWords(text))
  df = df.withColumn("nombre de mots", countWordsUDF(col("text")))

  // Afficher le DataFrame avec la nouvelle colonne
  df.show()

  // Utilisation de la pipeline pré-entraînée
  val explainDocumentPipeline = PretrainedPipeline("explain_document_ml")
  val annotations_df = explainDocumentPipeline.transform(df)
  annotations_df.show()

  //annotations_df.select("token.result").show(truncate=false)

  
  val finisher = new Finisher().setInputCols("document", "sentence", "token","spell","lemmas","stems","pos")
  val explainPipelineModel = explainDocumentPipeline.model

  val pipeline = new Pipeline().setStages(Array(
    explainPipelineModel,
    finisher
  ))

  val model = pipeline.fit(annotations_df)
  val annotations_dfNEW = model.transform(annotations_df)
  annotations_dfNEW.select("finished_token").show(truncate=false)
  

  // Utilisation de la pipeline pré-entraînée pour l'analyse de sentiments
  val sentimentPipeline = PretrainedPipeline("analyze_sentiment", lang = "en")
  val sentimentDF = sentimentPipeline.transform(df)

  // Extraction de la colonne "result" de l'analyse de sentiment et ajout en tant que nouvelle colonne "sentiment"
  val extractSentimentUDF = udf((sentiment: Seq[String]) => sentiment.headOption.getOrElse("neutral"))
  df = sentimentDF.withColumn("sentiment", extractSentimentUDF(col("sentiment.result")))

  // Afficher le DataFrame avec la nouvelle colonne de sentiment
  df.select("text", "nombre de mots", "sentiment").show(truncate = false)

  /*
  // Calcul du nombre total de mots
  val totalWords = df.select("nombre de mots").rdd.map(_.getInt(0)).sum()

  // Calcul du nombre total d'éléments dans le DataFrame
  val totalElements = df.count()

  // Calcul de la moyenne du nombre de mots par élément
  val averageWords = totalWords.toDouble / totalElements

  // Calcul de la médiane du nombre de mots par élément
  val wordCountsRDD = df.select("nombre de mots").rdd.map(_.getInt(0)).collect().sorted
  val n = wordCountsRDD.length
  val medianWords = if (n % 2 == 0) {
    val mid = n / 2
    (wordCountsRDD(mid - 1) + wordCountsRDD(mid)) / 2.0
  } else {
    wordCountsRDD(n / 2)
  }

  // Imprimer la médiane du nombre de mots par élément
  printGreen(f"Médiane du nombre de mots par élément : $medianWords%.2f")
  printGreen(f"Moyenne du nombre de mots par élément : $averageWords%.2f")
  */

  spark.stop()
}

