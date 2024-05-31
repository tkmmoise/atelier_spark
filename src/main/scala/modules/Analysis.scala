package modules

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StructType}
object Analysis {
  // Fonction pour compter le nombre de mots dans une chaîne
  def countWords(text: String): Int = text.split("\\s+").length

  // Ajout de la colonne "Number of books" au DataFrame
  def addWordCountColumn(booksDF: DataFrame): DataFrame = {
    val countWordsUDF = udf((text: String) => countWords(text))
    booksDF.withColumn("Number of words", countWordsUDF(col("books")))
  }

  def printStatistics(booksDF: DataFrame) = {
    val sentencesDF = booksDF.withColumn("sentences", explode(split(col("books"), "\\.")))
    val wordsDF = sentencesDF.withColumn("words", explode(split(col("sentences"), "\\s+")))
    val wordsPerSentenceDF = sentencesDF.withColumn("words_per_sentence", size(split(col("sentences"), "\\s+")))

    val numBooks = booksDF.count()
    val numSentences = sentencesDF.count()
    val numWords = wordsDF.count()
    val numUniqueWords = wordsDF.select("words").distinct().count()
    val meanWordsPerSentence = wordsPerSentenceDF.select(mean(col("words_per_sentence"))).first().getDouble(0)
    val medianWordsPerSentence = wordsPerSentenceDF.stat.approxQuantile("words_per_sentence", Array(0.5), 0.0).head

    println("# of books = ", numBooks)
    println("# of sentences = ", numSentences)
    println("# of words = ", numWords)
    println("# of unique words", numUniqueWords)
    println("mean # of words per sentence", meanWordsPerSentence)
    println("median # of words per sentence", medianWordsPerSentence)
  }

}
