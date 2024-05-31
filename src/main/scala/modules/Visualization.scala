package modules

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import plotly._
import plotly.element
import plotly.layout
import plotly.Plotly._
import plotly.layout.{Axis, Layout}
import plotly.Bar

object Visualization {

  // Histogramme du nombre de mots par livre breeze
//  def plotHistogram(wordsPerBookDF: DataFrame, spark: SparkSession): Unit = {
//    import spark.implicits._
//    val wordCounts = wordsPerBookDF.select("num_words").as[Int].collect()
//    val f = Figure()
//    val p = f.subplot(0)
//    p += hist(wordCounts, bins = 50)
//    p.title = "Histogramme du nombre de mots par livre"
//    p.xlabel = "Nombre de mots"
//    p.ylabel = "Nombre de livres"
//    f.saveas("histogram_words_per_book.png")
//  }

  // Histogramme du nombre de mots par livre
  def plotHistogram(wordsPerBookDF: DataFrame, spark: SparkSession): Unit = {
    import spark.implicits._
    val wordCounts = wordsPerBookDF.select("num_words").as[Int].collect().toSeq

    val hist = Histogram(wordCounts)
    val layout = Layout(title = "Histogram of Words per Book",
      xaxis = Axis(title = "Number of Words"),
      yaxis = Axis(title = "Frequency"))
    Plotly.plot("histogram_words_per_book.html", Seq(hist), layout)
  }

  // Top 10 des livres par nombre de mots
  def plotTop10Books(wordsPerBookDF: DataFrame, spark: SparkSession): Unit = {
    import spark.implicits._
    val top10Books = wordsPerBookDF.orderBy(desc("num_words")).limit(10).select("num_words").as[Int].collect().toSeq
    val bar = Bar((0 until 10).map(_.toString), top10Books)
    val layout = Layout(
      title = "Top 10 Books by Number of Words",
      xaxis = Axis(title = "Books"),
      yaxis = Axis(title = "Number of Words")
    )
    Plotly.plot("bar_words_per_book.html", Seq(bar), layout)
  }

}
