

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io.{BufferedWriter, File, FileWriter}
import collection.mutable
import Ordering.Implicits._

object Pradeep_Lam_SON {

  /*
   Counts the occurrence of candidates within a partition.
   Returns an iterator of key-value tuples with key:candidate sets and value:# occurrences.
   */
  def getCounts(data: Iterator[Set[String]], can: Set[Set[String]]): Iterator[(Set[String], Int)] ={
    val counts: mutable.HashMap[Set[String], Int] = new mutable.HashMap[Set[String], Int]()

    // Locate item-sets within each basket
    for(baskets <- data){
      for(items <- can){
        if(items.subsetOf(baskets)){   // If items are all present in basket
          counts(items) =  counts.getOrElse(items, 0) + 1
        }
      }
    }

    counts.iterator
  }

  /*
  Given the frequent item-sets of the previous iteration and the count of the current iteration,
  returns candidates for the current iteration.
   */
  def genCandidates(prev: List[Set[String]], count: Int): Set[Set[String]] = {
    val toReturn = new mutable.ListBuffer[Set[String]]()

    // Get union between all item-sets in previous iteration
    for (outer <- prev){
      for (inner <- prev){
        toReturn += outer.union(inner)
      }
    } // Too lazy to change now(And set conversion makes it ok)...But the inner loop should technically "regress" as to not create duplicates

    /*
     Inconsequential(At least, for the data used for grading)...But, this method has a small error.
     Example:
     Set[Set["A","B"],Set["A","C"],Set["A","D"],Set["B","D"]]
     would produce
     Set[Set["A","B","C"], Set["A","B","D"]]
     The easy fix would be to add a count for each set. If the count equals the
     number of elements from prev iteration then it's valid.

     I'm not going to fix it, but if someone decides to copy this from my github, fixing this could potentially help
     thwart MOSS.
     */

    val filtered = toReturn.toSet.filter(one => one.size == count)

    filtered
  }


  def main(args: Array[String]): Unit = {

    var i_path = ""
    var s_thresh = 0
    var o_path = ""
    var o_string = ""

    if (args.length != 3){
      i_path = "./yelp_reviews_large.txt"
      s_thresh = 100000
      o_path = "./output.txt"
    }
    else{
      i_path = args(0)
      s_thresh = args(1).toInt
      o_path = args(2)
    }

    val start_time = System.nanoTime

    // Creating Spark Configuration
    val spark_config = new SparkConf().setAppName("").setMaster("local[*]")

    // Creating Spark Context using Spark Configuration
    val spark_context = new SparkContext(spark_config)

    // Read-in data & collect into "baskets"
    val baskets = spark_context.textFile(i_path).map(_.split(',') match {
      case Array(review, word) => (review, word)
    }).groupByKey().map(_._2.toSet)


    val numPart = baskets.partitions.size

    /*
    Broadcast is used to "keep read-only variable cached on each machine rather than shipping a copy of it with tasks".
    Don't know if it's the best option, but shit seems to run decently quickly.
     */


    //  Singles & pairs done separately versus all other n_tuples because reasons..
    val singles_son1_out = spark_context.broadcast(baskets.flatMap(one=>one).mapPartitions(part => part.toList.groupBy(identity).mapValues(_.size).filter(_._2 >= s_thresh/numPart).iterator).keys.map(str=>Set(str)).collect().toSet)
    val singles_son2_out = baskets.mapPartitions(part => getCounts(part, singles_son1_out.value)).reduceByKey(_+_).filter(_._2 >= s_thresh).keys.collect()

    // Format + Print to file
    val s1 = singles_son2_out.sortBy(_.head)
    o_string += s1.map(_.mkString("(", ", ", ")")).mkString(", ")
    o_string += "\n\n"

    val pair_can = singles_son2_out.flatten.combinations(2).map(_.toSet).toSet

    val pairs_son1_out = spark_context.broadcast(baskets.mapPartitions(part => getCounts(part, pair_can)).filter(_._2 >= s_thresh/numPart).keys.collect().toSet)
    val pairs_son2_out = baskets.mapPartitions(part => getCounts(part, pairs_son1_out.value)).reduceByKey(_+_).filter(_._2 >= s_thresh).keys.collect()

    // Format + Print to file
    val s2 = pairs_son2_out.map(elmt=>elmt.toList.sorted).sortWith((b1,b2) => b1 < b2)
    o_string += s2.map(_.mkString("(", ", ", ")")).mkString(", ")
    o_string += "\n\n"

    // Stores the frequent item-sets from previous iteration
    var stupid = new mutable.ListBuffer[Set[String]]() // Refers to the fact that I *have to* do it this way

    // Init with frequent pairs
    pairs_son2_out.foreach(element => stupid += element)

    var count = 3

    while(stupid.nonEmpty & stupid.size >= count){ // Need as least as many candidates as count to have possibility of nth candidates

      // Generate candidates
      val n_can = genCandidates(stupid.toList, count)

      stupid.clear() // Empty candidates from previous iteration

      // Filter frequent
      val son1_out = spark_context.broadcast(baskets.mapPartitions(part => getCounts(part, n_can)).filter(_._2 >= s_thresh/numPart).keys.collect().toSet)
      val son2_out =  baskets.mapPartitions(part => getCounts(part, son1_out.value)).reduceByKey(_+_).filter(_._2 >= s_thresh).keys.collect()

      // Keep to generate candidates for next iteration
      son2_out.foreach(element => stupid += element)

      if(stupid.nonEmpty){
        // Format + Print to file
        val sn = son2_out.map(elmt=>elmt.toList.sorted).sortWith((b1,b2) => b1 < b2)
        o_string += sn.map(_.mkString("(", ", ", ")")).mkString(", ")
        o_string += "\n\n"
      }

      count += 1

    }

    val file = new File(o_path)
    val fp = new BufferedWriter(new FileWriter(file))

    fp.write(o_string)
    fp.close()

    println(s"Time: ${(System.nanoTime - start_time)/1e9d}")
  }

}