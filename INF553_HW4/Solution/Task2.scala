

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.clustering.BisectingKMeans
import org.apache.spark.mllib.linalg.{Vector, Vectors}


object Task2 {

  def outToFile(WSSE: Double, k_sizes: Array[Int], k_error: Array[Double], k_top_ten: Array[Array[String]], algo: String, n: Int): Unit ={
    var output = "{\n"

    var print_algo = ""
    if(algo == "K"){
      print_algo = "K-Means"
    }
    else{
      print_algo = "Bisecting K-Means"
    }

    output += "\t\"algorithm\":\"" + print_algo +  "\",\n"
    output += "\t\"WSSE\":" + WSSE.toString + ",\n"
    output += "\t\"clusters\":[\n"


    for(k <- k_sizes.indices){
      output += "\t{\n"
      output += "\t\t\"id\":" + (k + 1).toString + ",\n"
      output += "\t\t\"size\":" + k_sizes(k).toString + ",\n"
      output += "\t\t\"error\":" + k_error(k).toString + ",\n"
      output += "\t\t\"terms\":" + k_top_ten(k).mkString("[\"", "\",\"", "\"]") + "\n"
      output += "\t},\n"
    }

    if(k_sizes.length < n){
      output = output.replaceAll(",\n$", "\n")
    }

    //Insert empty centroids to make n
    for(k <- k_sizes.length until n){
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

  def calcError(coord: Vector, centroid: Vector): Double = {
    (coord.toArray zip centroid.toArray).map { case (v1,v2) => Math.pow(v1 - v2, 2) }.sum
  }

  def standard_tfidf(encode: Map[String,Int], count_dict: Map[String,Int], review: Array[String]): Vector = {
    val standard = Array.fill(encode.size)(0.0)
    val n_docs = count_dict.size

    val word_count = review.groupBy(identity).mapValues(_.size) // Mapping of words within review to count

    // Store word count in encoded place
    for (w <- word_count.keys){
      val tf = word_count(w) // Number of times word appears in document
      val idf = (n_docs + 1.0)/(count_dict(w) + 1.0)    // Proportional to number of times word appears in corpus
      standard(encode(w)) = tf*math.log(idf)
    }

    Vectors.dense(standard)
  }

  def main(args: Array[String]): Unit = {
    var input = "./yelp_reviews_clustering_small.txt" // Input text-file
    var algo = "B" // K for KMeans and B for Bisecting KMeans
    var n = 8 // Number of clusters
    var max_iter = 20 // Max number of iterations

    if (args.length == 4){
      input = args(0)
      algo = args(1)
      n = args(2).toInt
      max_iter = args(3).toInt
    }

    // Creating Spark Configuration
    val spark_config = new SparkConf().setAppName("").setMaster("local[*]")

    // Creating Spark Context using Spark Configuration
    val spark_context = new SparkContext(spark_config)

    // Load in reviews
    val reviews = spark_context.textFile(input).map(_.split(" "))

    // Compute term-frequency
    // Create a dict containing vocab from the entire corpus & Number of documents containing respective word
    val count_dict = reviews.collect().flatMap(_.toSet).groupBy(identity).mapValues(_.size).map(identity)

    // Used to "encode" & "decode" word dict
    val encode = count_dict.keys.toList.sorted.zipWithIndex.toMap
    val decode = encode.map{case (k,v) => (v,k)}

    // Map data into form (Cluster #, Int Array)
    var tfidf =  reviews.map(review => standard_tfidf(encode,count_dict,review))


    if(algo == "K"){
      val km = new KMeans().setK(n).setMaxIterations(max_iter).setSeed(42)
      val clusters = km.run(tfidf)
      val WSSE = clusters.computeCost(tfidf)
      val k_sizes = clusters.predict(tfidf).groupBy(identity).mapValues(_.size).collect().sortBy(_._1).map(_._2) // Cluster & size

      val k_center = clusters.clusterCenters

      val test_this = tfidf.map(v => clusters.predict(v))

      val k_error = tfidf.map{v => (clusters.predict(v), calcError(v, k_center(clusters.predict(v))))}.groupByKey().mapValues(_.sum).collect().sortBy(_._1).map(_._2)

      val k_top_ten = k_center.zipWithIndex.map{case(rep,k) => (k, rep.toArray.zipWithIndex.sortBy(-_._1).take(10).map{case(useless,word) => decode(word)})}.sortBy(_._1).map(_._2)

      outToFile(WSSE, k_sizes, k_error, k_top_ten, "K", n)
    }

    else{
      val bkm = new BisectingKMeans().setK(n).setMaxIterations(max_iter).setSeed(42)
      val clusters = bkm.run(tfidf)
      val WSSE = clusters.computeCost(tfidf)
      val k_sizes = clusters.predict(tfidf).groupBy(identity).mapValues(_.size).collect().sortBy(_._1).map(_._2) // Cluster & size

      val k_center = clusters.clusterCenters

      val k_error = tfidf.map{v => (clusters.predict(v), calcError(v, k_center(clusters.predict(v))))}.groupByKey().mapValues(_.sum).collect().sortBy(_._1).map(_._2)

      val k_top_ten = k_center.zipWithIndex.map{case(rep,k) => (k, rep.toArray.zipWithIndex.sortBy(-_._1).take(10).map{case(useless,word) => decode(word)})}.sortBy(_._1).map(_._2)

      outToFile(WSSE, k_sizes, k_error, k_top_ten, "W", n)
    }




  }

}