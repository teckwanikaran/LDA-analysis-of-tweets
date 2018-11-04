// Databricks notebook source
// MAGIC %md
// MAGIC ## LDA analysis of Tweets (Spark streaming)
// MAGIC ### Notebook by [Karan Teckwani](https://karanteckwani.com)
// MAGIC 
// MAGIC - [LinkedIn](https://www.linkedin.com/in/karanteckwani/)
// MAGIC - [Github](https://github.com/teckwanikaran)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Importing the Libraries

// COMMAND ----------

import org.apache.spark._
import org.apache.spark.storage._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import scala.collection.mutable
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

// COMMAND ----------

// MAGIC %md
// MAGIC ### Importing the Tweets

// COMMAND ----------

// MAGIC %md
// MAGIC #### Setup the Twitter connection tokens and secure keys

// COMMAND ----------

dbutils.widgets.text("consumerKey", "WCODh7F3kZMQ8gO0GjPDCJrsJ", label = "Twitter_consumerKey")
dbutils.widgets.text("consumerSecret", "8Qc7oUYVqQtdGXDgljxld4K8h8bSQ8tPV7L1W6kb8nRaD65WSx", label = "Twitter_consumerSecret")
dbutils.widgets.text("accessToken", "182712593-Dt4j5Ag2LmS8QHtS3EoCXhSU7EnT56E9JSa6wttV", label = "Twitter_accessToken")
dbutils.widgets.text("accessTokenSecret", "hti6WMpcARlhAwkciwqObGfbZWs0S4rJmR4yuaiEZ5zDh", label = "Twitter_accessTokenSecret")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Setup the import directory and make sure the directory is empty

// COMMAND ----------

val tweetsDirectory = "/Tweets"
val hashTagDirectory = "/Hashtag"
dbutils.fs.rm(tweetsDirectory, true)
dbutils.fs.rm(hashTagDirectory, true)

// COMMAND ----------

// Setup the time interval for batch streaming and setup the streaming content using StreamingContext
val slideInterval = new Duration(1 * 1000) // 1 second interval between each request
val ssc = new StreamingContext(sc, slideInterval)

//Setup the Twitter connection tokens and secure keys
//dbutils.widgets.text("consumerKey", "key", label = "Twitter_consumerKey")
//dbutils.widgets.text("consumerSecret", "key", label = "Twitter_consumerSecret")
//dbutils.widgets.text("accessToken", "key", label = "Twitter_accessToken")
//dbutils.widgets.text("accessTokenSecret", "key", label = "Twitter_accessTokenSecret")

// Setup the connection keys into System Property
System.setProperty("twitter4j.oauth.consumerKey", dbutils.widgets.get("consumerKey"))
System.setProperty("twitter4j.oauth.consumerSecret", dbutils.widgets.get("consumerSecret"))
System.setProperty("twitter4j.oauth.accessToken", dbutils.widgets.get("accessToken"))
System.setProperty("twitter4j.oauth.accessTokenSecret", dbutils.widgets.get("accessTokenSecret"))

// Creating the builder to estabilish the connection with the Twitter application
val auth = Some(new OAuthAuthorization(new ConfigurationBuilder().build()))

// Creating the twitterStream using the TwitterUtils library
val twitterStream = TwitterUtils.createStream(ssc, auth)

// Parse the tweets and filter only the English Tweets
val EnglishTweets = twitterStream.filter(_.getLang() == "en")
  
var numTweetsCollected = 0L // Tracks number of tweets collected
var numHashTagsCollected = 0L // Tracks number of tweets collected
val partitionsEachInterval = 1 // The number of partitions in each RDD of tweets in the DStream
  
// Extract the status/Text of tweets and split into words
val status = EnglishTweets.map(status => status.getText())
val words = status.flatMap(status => status.split(" "))
words.foreachRDD((rdd, time) => {
  val count = rdd.count()
    if (count > 0){
      val outputRDD = rdd.repartition(partitionsEachInterval) // repartition every 1 seconds
      outputRDD.saveAsTextFile(tweetsDirectory + "/tweets_" + time.milliseconds.toString +".txt") // save as textfile
      numTweetsCollected += count // update with the latest count
      }
  })

// Parse the tweets and gather the hashTags.
val hashTagStream = EnglishTweets.map(_.getText).flatMap(_.split(" ")).filter(_.startsWith("#"))
hashTagStream.foreachRDD((rdd, time) => {
  val count = rdd.count()
    if (count > 0){
      val outputRDD = rdd.repartition(partitionsEachInterval) // repartition every 1 seconds
      outputRDD.saveAsTextFile(hashTagDirectory + "/tweets_" + time.milliseconds.toString +".txt") // save as textfile
      numHashTagsCollected += count // update with the latest count
      }
  })

// COMMAND ----------

ssc.start() // Ran for 20 minutes

// COMMAND ----------

numTweetsCollected   //Reexecution will reveal the number of Tweets collected

// COMMAND ----------

numHashTagsCollected //Reexecution will reveal the number of HashTags collected

// COMMAND ----------

// Execute this cell to stop the streaming context
ssc.stop(stopSparkContext = false)
// Stoping the backgroud streaming
StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) }

// COMMAND ----------

// MAGIC %md
// MAGIC ### Tweets LDA Model

// COMMAND ----------

display(dbutils.fs.ls("dbfs:/Tweets/"))

// COMMAND ----------

// MAGIC %md 
// MAGIC #### Text Cleaning, Tokenizing the words and building the Vocabulary

// COMMAND ----------

// Load documents from text files, 1 document per file
val tweetdata: RDD[String] = sc.wholeTextFiles("dbfs:/Tweets/*.txt/").map(_._2)

val stopWords = sc.textFile("/tmp/stopwords").collect()

// Flatten, collect, and broadcast.
val stopWordsoutput = stopWords.flatMap(x => x.split(",")).map(_.trim)

// Split using a regular expression that extracts words
val wordsWithStopWordsfiltered: RDD[String] = tweetdata.flatMap(x => x.split("\\W+"))
wordsWithStopWordsfiltered.filter(!stopWordsoutput.contains(_))

// COMMAND ----------

// Split each document into a sequence of terms (words)
val tokenized: RDD[Seq[String]] =
  wordsWithStopWordsfiltered.map(_.toLowerCase.split("\\s")).map(_.filter(_.length > 3).filter(_.forall(java.lang.Character.isLetter)))

// COMMAND ----------

// Choose the vocabulary.
// termCounts: Sorted list of (term, termCount) pairs
val termCounts: Array[(String, Long)] =
  tokenized.flatMap(_.map(_ -> 1L)).reduceByKey(_ + _).collect().sortBy(-_._2)

// COMMAND ----------

// vocabArray: Chosen vocab (removing common terms)
val numStopwords = 20
val vocabArray: Array[String] =
  termCounts.takeRight(termCounts.size - numStopwords).map(_._1)
//   vocab: Map term -> term index
val vocab: Map[String, Int] = vocabArray.zipWithIndex.toMap

// Convert documents into term count vectors
val documents: RDD[(Long, Vector)] =
  tokenized.zipWithIndex.map { case (tokens, id) =>
    val counts = new mutable.HashMap[Int, Double]()
    tokens.foreach { term =>
      if (vocab.contains(term)) {
        val idx = vocab(term)
        counts(idx) = counts.getOrElse(idx, 0.0) + 1.0
      }
    }
    (id, Vectors.sparse(vocab.size, counts.toSeq))
  }

// COMMAND ----------

// MAGIC %md 
// MAGIC #### LDA Model Tweets Clusters

// COMMAND ----------

// Set LDA parameters
val numTopics = 3
val lda = new LDA().setK(numTopics).setMaxIterations(200)

val ldaModel = lda.run(documents)

// Print topics, showing top-weighted 10 terms for each topic.
val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 5)
topicIndices.foreach { case (terms, termWeights) =>
  println("TOPIC:")
  terms.zip(termWeights).foreach { case (term, weight) =>
    println(s"${vocabArray(term.toInt)}\t$weight")
  }
  println(s"==========")
}

// COMMAND ----------

// MAGIC %md 
// MAGIC ### HashTags LDA Model

// COMMAND ----------

display(dbutils.fs.ls("dbfs:/Hashtag/"))

// COMMAND ----------

// Load documents from text files, 1 document per file
val hashTagdata: RDD[String] = sc.wholeTextFiles("dbfs:/Hashtag/*.txt/").map(_._2)

// Split each document into a sequence of terms (words)
val tokenized: RDD[Seq[String]] =
  hashTagdata.map(_.toLowerCase.split("\\s")).map(_.filter(_.length > 3).filter(_.forall(java.lang.Character.isLetter)))

// Choose the vocabulary.
// termCounts: Sorted list of (term, termCount) pairs
val termCounts: Array[(String, Long)] =
  tokenized.flatMap(_.map(_ -> 1L)).reduceByKey(_ + _).collect().sortBy(-_._2)

// COMMAND ----------

// vocabArray: Chosen vocab (removing common terms)
val vocabArray: Array[String] =
  termCounts.takeRight(termCounts.size ).map(_._1)
//   vocab: Map term -> term index
val vocab: Map[String, Int] = vocabArray.zipWithIndex.toMap

// Convert documents into term count vectors
val documents: RDD[(Long, Vector)] =
  tokenized.zipWithIndex.map { case (tokens, id) =>
    val counts = new mutable.HashMap[Int, Double]()
    tokens.foreach { term =>
      if (vocab.contains(term)) {
        val idx = vocab(term)
        counts(idx) = counts.getOrElse(idx, 0.0) + 1.0
      }
    }
    (id, Vectors.sparse(vocab.size, counts.toSeq))
  }

// COMMAND ----------

// Set LDA parameters
val numTopics = 3
val lda = new LDA().setK(numTopics).setMaxIterations(200)
val ldaModel = lda.run(documents)

// Print topics, showing top-weighted 10 terms for each topic.
val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 5)
topicIndices.foreach { case (terms, termWeights) =>
  println("TOPIC:")
  terms.zip(termWeights).foreach { case (term, weight) =>
    println(s"${vocabArray(term.toInt)}\t$weight")
  }
  println(s"==========")
}

// COMMAND ----------


