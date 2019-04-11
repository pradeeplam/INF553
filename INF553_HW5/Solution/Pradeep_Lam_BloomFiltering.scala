
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter._
import scala.util.hashing.MurmurHash3
import scala.collection.mutable.ListBuffer

object BloomFiltering {
  def main(args: Array[String]): Unit = {

    var consumerKey = "BiGbKhqBbdrX0MAsqn6S1m5Mn"
    var consumerKeySecret = "5UQ8hhJIEQIhsGK90CYnou2E5yUnfC7bRAof5N8Q9AsQjd8mbt"
    var accessToken = "1068693791350022144-pJQCp859vQaX3ErOGFTePD8DlVXO20"
    var accessTokenSecret = "VuVeDbk8Iq0i7QEedyoCwhr96pzqX2RrEU7HO2peQEFMi"

    if (args.length == 4) {
      consumerKey = args(0)
      consumerKeySecret = args(1)
      accessToken = args(2)
      accessTokenSecret = args(3)
    }

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerKeySecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    // Creating Spark Configuration
    val spark_config = new SparkConf().setAppName("").setMaster("local[*]")

    // RDD will be generated from received data at n second intervals
    val stream_context = new StreamingContext(spark_config, Seconds(10))

    stream_context.sparkContext.setLogLevel(logLevel = "OFF")

    val stream = TwitterUtils.createStream(stream_context, None, Array("anime","manga","cartoons", "tv", "books"))

    val toCompare = new ListBuffer[String]()

    val filterLength = Math.pow(10, 6).toInt
    val bloom = Array.fill(filterLength){false}

    var correct = 0
    var incorrect = 0

    stream.foreachRDD(partial => {
      if(partial.count() != 0) {

        val collected = partial.collect()

        for (item <- collected) {

          val hashtags = item.getHashtagEntities

          for (tag <- hashtags) {
            val asString = tag.getText

            // Hash hash baby 1
            val hashed1 = Math.abs(MurmurHash3.stringHash(asString, 69)) % filterLength

            // Hash hash baby 2
            val hashed2 = Math.abs(MurmurHash3.stringHash(asString, 420)) % filterLength

            // Hash hash baby 3
            val hashed3 = Math.abs(MurmurHash3.stringHash(asString, 1080)) % filterLength

            // Has hastag been seen before based on bloom filter
            val check = bloom(hashed1) & bloom(hashed2) & bloom(hashed3)

            // Has the hastag been seen before (Ground truth)
            val truth = toCompare.contains(asString)

            // Collect stats
            if (truth != check) {
              incorrect += 1
            }
            else {
              correct += 1
            }

            // Print stats
            println(s"Number of correct pred (So far): $correct")
            println(s"Number of incorrect pred (So far): $incorrect")
            println(s"False positives: ${incorrect / (correct + incorrect).toFloat}")

            // Update bitmap & ground truth if necessary
            if (!truth) {
              bloom(hashed1) = true
              bloom(hashed2) = true
              bloom(hashed3) = true
              toCompare += asString
            }

          }

        }
      }

    })


    stream_context.start()
    stream_context.awaitTermination()

  }

}
