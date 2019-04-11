

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object Task1 {


  def calcError(coord: Array[Double], centroid: Array[Double]): Double = {
    (coord zip centroid).map { case (v1,v2) => Math.pow(v1 - v2, 2) }.sum
  }

  def assignCluster(centroids: Array[Array[Double]], review: Array[Double]): Int = {
    var assignment = -1
    var min_distance = Double.MaxValue

    val n_words = centroids(0).length

    for(k <- centroids.indices){
      var dist = 0.0
      for(i <- 0 until n_words){
        dist += scala.math.pow(review(i) - centroids(k)(i),2)
      }
      if(min_distance > dist){
        assignment = k
        min_distance = dist
      }

    }

    assignment
  }

  def computeCentroid(coord : Iterable[Array[Double]]): Array[Double] = {
    val listed_coord = coord.toList

    val new_centroid = Array.fill(listed_coord.head.length)(0.0)

    for(one <- listed_coord) {

      for (i <- one.indices) {
        new_centroid(i) += one(i) / listed_coord.length
      }

    }

    new_centroid
  }

  def standard_tfidf(encode: Map[String,Int], count_dict: Map[String,Int], review: Array[String]): Array[Double] = {
    val standard = Array.fill(encode.size)(0.0)
    val n_docs = count_dict.size

    val word_count = review.groupBy(identity).mapValues(_.size) // Mapping of words within review to count

    // Store word count in encoded place
    for (w <- word_count.keys){
      val tf = word_count(w) // Number of times word appears in document
      val idf = (n_docs + 1.0)/(count_dict(w) + 1.0)    // Proportional to number of times word appears in corpus
      standard(encode(w)) = tf*math.log(idf)
    }

    standard
  }


  def standard_wc(encode: Map[String,Int], review: Array[String]): Array[Double] = {
    val standard = Array.fill(encode.size)(0.0)

    val word_count = review.groupBy(identity).mapValues(_.size) // Mapping of words within review to count

    // Store word count in encoded place
    for (w <- word_count.keys){
      standard(encode(w)) = word_count(w)
    }

    standard
  }


  def main(args: Array[String]): Unit = {
    var input = "./yelp_reviews_clustering_small.txt" // Input text-file
    var feature = "T" // W for word count, T for TF-IDF
    var n = 5 // Number of clusters
    var max_iter = 20 // Max number of iterations

    if (args.length == 4){
      input = args(0)
      feature = args(1)
      n = args(2).toInt
      max_iter = args(3).toInt
    }

    // Creating Spark Configuration
    val spark_config = new SparkConf().setAppName("").setMaster("local[*]")

    // Creating Spark Context using Spark Configuration
    val spark_context = new SparkContext(spark_config)

    // Read in data as array of words
    val data = spark_context.textFile(input).map(_.split(" "))

    // Create a dict containing vocab from the entire corpus & Number of documents containing respective word
    val count_dict = data.collect().flatMap(_.toSet).groupBy(identity).mapValues(_.size).map(identity)

    // Used to "encode" & "decode" word dict
    val encode = count_dict.keys.toList.sorted.zipWithIndex.toMap
    val decode = encode.map{case (k,v) => (v,k)}


    // Map data into form (Cluster #, Int Array)
    var clustered = if (feature == "W") data.map(review => (0,standard_wc(encode,review))) else data.map(review => (0,standard_tfidf(encode,count_dict,review)))


    // Create n random centroids (Using datapoints from within)
    var centroids = clustered.takeSample(false, n, 20181031).map(_._2)

    // Clarification
    // "To"     -> [start,end]
    // "Until"  -> [start,end)

    // K-Means iterations
    for(_ <- 1 until  max_iter){
      clustered = clustered.map{case (k,v) => (assignCluster(centroids, v),v)} // Will replace w/ closest cluster for present iteration
      centroids = clustered.groupByKey().mapValues(coord => computeCentroid(coord)).sortByKey().values.collect()
    }

    clustered = clustered.map{case (k,v) => (assignCluster(centroids, v),v)}

    // Calculations needed for output
    val part = clustered.map{case(k, coord) => (k,calcError(coord, centroids(k)))}
    val WSSE = part.map(_._2).sum()
    val k_error = part.groupByKey().map{case(k,v) => (k, v.sum)}.sortByKey().values.collect()
    val k_sizes = clustered.groupByKey().mapValues(_.size).sortByKey().values.collect()
    val k_top_ten = centroids.zipWithIndex.map{case(rep,k) => (k, rep.zipWithIndex.sortBy(-_._1).take(10).map{case(useless,word) => decode(word)})}.sortBy(_._1).map(_._2)

    var output = "{\n"

    output += "\t\"algorithm\":\"K-Means\",\n"
    output += "\t\"WSSE\":" + WSSE.toString + ",\n"
    output += "\t\"clusters\":[\n"


    for(k <- centroids.indices){
      output += "\t{\n"
      output += "\t\t\"id\":" + (k + 1).toString + ",\n"
      output += "\t\t\"size\":" + k_sizes(k).toString + ",\n"
      output += "\t\t\"error\":" + k_error(k).toString + ",\n"
      output += "\t\t\"terms\":" + k_top_ten(k).mkString("[\"", "\",\"", "\"]") + "\n"
      output += "\t},\n"
    }

    if(centroids.length < n){
      output = output.replaceAll(",\n$", "\n")
    }

    //Insert empty centroids to make n
    for(k <- centroids.length until n){
      output += "\t{\n"
      output += "\t\t\"id\":" + (k + 1).toString + ",\n"
      output += "\t\t\"size\":0,\n"
      output += "\t\t\"error\":0,\n"
      output += "\t\t\"terms\":[]\n"
      output += "\t},\n"
    }

    output = output.replaceAll(",\n$", "\n")

    output += "\t]\n"
    output += "}"

    val file = new File("out.json")
    val fp = new BufferedWriter(new FileWriter(file))

    fp.write(output)
    fp.close()
  }

}