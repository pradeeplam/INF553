
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter._
import twitter4j.Status
import scala.collection.mutable.ListBuffer

object TwitterStreaming {

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

    val stream = TwitterUtils.createStream(stream_context, None, Array("anime","manga","cartoons"))

    val reservoir = new ListBuffer[Status]()
    var count = 0
    val r = scala.util.Random

    stream.foreachRDD(partial => {

      if(partial.count() != 0){
        val collected = partial.collect()

        for(item <- collected){
          if(reservoir.length != 100){
            reservoir += item
          }
          else{
            val replaceRand = r.nextInt(count)

            if(replaceRand < 100){
              val which = r.nextInt(100)
              reservoir.update(which, item)

              println(s"The number of the twitter from beginning: $count" )
              println("Top 5 hot hashtags: ")

              val hashtags = reservoir.flatMap(_.getHashtagEntities.map(_.getText))
                .groupBy(identity).mapValues(_.size).toList.sortBy(-_._2)

              for(top <- hashtags.take(5)){
                val name = top._1
                val count = top._2

                println(s"$name:$count")
              }

              val avgSize = reservoir.map(_.getText.length).sum/100.0

              println(s"The average length of the twitter is: $avgSize")

            }

          }

          count += 1
        }

      }
    })


    stream_context.start()
    stream_context.awaitTermination()

  }

}