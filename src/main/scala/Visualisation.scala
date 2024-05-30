package example
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions => F}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import plotly._, plotly.element._, plotly.layout._, plotly.Plotly._

object Visualisation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Visualisation des données")
      .master("local[*]") // "local[*]" pour exécuter Spark en mode local avec tous les cœurs disponibles
      .getOrCreate()

    // Importer les implicites nécessaires pour les conversions
    import spark.implicits._

    // Obtenir le contexte Spark
    val sc = spark.sparkContext

    // Lire le fichier texte
    val files1 = sc.textFile("./books_large_p1.txt")
    val mergedFile = files1.take(100000)

    // Diviser le texte en segments à partir de "it"
    var counter: Int  = 0
    def indexCalulate(): Int = {
      counter+=1
      return counter
    }

    val segments = mergedFile.map(line =>
      if(line.contains("isbn"))
        (line , indexCalulate())
      else
        (line, counter)
    )

    // Créer un RDD de paires clé-valeur
    val rdd = spark.sparkContext.parallelize(segments)

    // Utiliser reduceByKey pour concaténer les phrases par clé
    val concatenatedRDD = rdd.map { case (text, key) => (key, text) }
      .reduceByKey((a, b) => a + " " + b)

    val textRDD = concatenatedRDD.map{case(_, text)=> Row(text)}

    // Définir le schéma pour le DataFrame
    val schema = new StructType().add(StructField("books", StringType, nullable = false))

    // Créer un DataFrame à partir du RDD et du schéma
    val df = spark.createDataFrame(textRDD, schema)

    // Définir une fonction UDF pour compter les mots
    val wordCount: UserDefinedFunction = F.udf((text: String) => text.split("\\s+").length)

    // Appliquer la fonction UDF pour ajouter une nouvelle colonne avec le nombre de mots
    val resultDF = df.withColumn("word_count", wordCount($"books"))

    // Filtrer les lignes où le nombre de mots est inférieur à 30
    val booksDF = resultDF.filter($"word_count" >= 5000)

    val lengthCounts = booksDF.groupBy("word_count").count().collect()

    val lengths = lengthCounts.map(_.getAs[Int]("word_count")).toSeq
    val counts = lengthCounts.map(_.getAs[Long]("count")).toSeq

    // 6. Créer l'histogramme
    val hist = Bar(x = lengths, y = counts)

    // Définir le layout du graphique
    val layout = Layout(
      title = "Histogramme des longueurs de texte",
      xaxis = Axis(title = "Longueur du texte"),
      yaxis = Axis(title = "Nombre d'occurrences")
    )

    // Générer un fichier html appellé histogram.html dans la même répértoire pour pouvoir visualiser à l'aide de notre navigateur
    plot(path = "histogram.html", traces = Seq(hist), layout = layout, useCdn = false, openInBrowser = true, addSuffixIfExists = true)


    // Fermer la session Spark
    spark.stop()

  }
}
