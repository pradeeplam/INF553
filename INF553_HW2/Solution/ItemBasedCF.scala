
import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object ItemBasedCF {


  def calcMean( data: Iterable[(String,Double)]) = {
    val ratings = data.map(x => x._2)

    ratings.sum/ratings.size
  }


  def calcGroup(r1 : Double, r2: Double) = {
    val d = math.abs(r1 - r2)

    if (d >= 0.0 & d < 1.0)
      ">=0 and <1"
    else if (d >=1.0 & d < 2.0)
      ">=1 and <2"
    else if (d >=2.0 & d < 3.0)
      ">=2 and <3"
    else if (d >=3.0 & d < 4.0)
      ">=3 and <4"
    else
      ">=4"
  }

  def calcSim( business1: Iterable[(String,Double)], business2: Iterable[(String,Double)]) = {

    val keys1 = business1.map(_._1).toList
    val keys2 = business2.map(_._1).toList

    val int_keys = keys1 intersect keys2
    val filtered1 = business1.filter{case (k,v) => int_keys.contains(k)}.toList.sortBy{case (k,v) => k}.map(_._2)
    val filtered2 = business2.filter{case (k,v) => int_keys.contains(k)}.toList.sortBy{case (k,v) => k}.map(_._2)

    if(int_keys.length == 1){  // Only 1 user is common between both businesses
      0.0
    }
    else{
      val mean1 = filtered1.sum/filtered1.size
      val mean2 = filtered2.sum/filtered2.size

      val num1 = filtered1.map(num => num - mean1)
      val num2 = filtered2.map(num => num - mean2)

      val denom1 = math.sqrt(filtered1.map(num => math.pow(num - mean1,2)).sum)
      val denom2 = math.sqrt(filtered2.map(num => math.pow(num - mean2,2)).sum)

      val top = num1.zip(num2).map(num => num._1*num._2).sum
      val bot = denom1 * denom2

      if(bot == 0.0){ // Ex all of the raters rate the same
        0.0
      }
      else{
        top/bot
      }
    }

  }

  def makePred( toUse: Iterable[(Double,Double)], uAvg: Double, bAvg: Double, tAvg: Double) = {
    val filtered = toUse.filter{case (rate,cosim) => cosim > 0.0}

    if(filtered.isEmpty) { // There is nothing we can work with in terms of similarity
      (bAvg + uAvg)/2 // Average business avg and user avg
    }
    else{
      val num = filtered.map{case (rate,cosim) => rate*cosim}.sum
      val denom = filtered.map{case (rate,cosim) => math.abs(cosim)}.sum

      // Weigh down the prediction if it's based on only a few item comparisons.
      /*
      Another logical way to do it that works!

      if(filtered.size < 5){
        3.0/2 + num/(2*denom) # Avg of 3 and the predicted rating (Skew towards 3 (Middle rating))
      }
      else{
        num/denom
      }
      */

      if(filtered.size < 5){
        val beta = 0.6 // Hyper-param but 0.5 works as well
        tAvg*beta + (1-beta)*(num/denom)
      }
      else{
        num/denom
      }
    }
  }

  def main(args: Array[String]): Unit = {

    var  train_path = ""
    var test_path = ""

    if (args.length != 2){
      train_path = "./train_review.csv"
      test_path = "./test_review.csv"
    }
    else{
      train_path = args(0)
      test_path = args(1)
    }

    val start_time = System.nanoTime

    // Creating Spark Configuration
    val spark_config = new SparkConf().setAppName("").setMaster("local[2]")

    // Creating Spark Context using Spark Configuration
    val spark_context = new SparkContext(spark_config)

    val train_data = spark_context.textFile(train_path).filter(row => row != "user_id,business_id,stars")
    val test_data = spark_context.textFile(test_path).filter(row => row != "user_id,business_id,stars")

    val train_ratings = train_data.map(_.split(',') match {
      case Array(user, business, rate) => (user, business, rate.toDouble)
    })

    val test_ratings = test_data.map(_.split(',') match {
      case Array(user, business, rate) => (user, business, rate.toDouble)
    })

    // Inefficient as hell but w/e..Spent enough time trying to make distributed
    // Create hash maps to lists of items/users for user/item
    val item_map = train_ratings.map{case (user,business,rate) => (business,(user,rate))}.groupByKey().collectAsMap()
    val user_map = train_ratings.map{case (user,business,rate) => (user,(business,rate))}.groupByKey().collectAsMap()

    // Have to filter the test data for the cold-start problem
    /*
    These cases may come in the form of:
    1. Cold-Start users
    2. Cold-Start businesses
    3. Cold-Start users & businesses
    It would be smart to filter out these cases and manually take care of them.
    */

    val test_given = test_ratings.filter{case (user, business, rate) =>  user_map.contains(user) & item_map.contains(business)}

    val test_cs1 = test_ratings.filter{case (user, business, rate) =>  !user_map.contains(user) & item_map.contains(business)}
    val test_cs2 = test_ratings.filter{case (user, business, rate) =>  user_map.contains(user) & !item_map.contains(business)}
    val test_cs3 = test_ratings.filter{case (user, business, rate) =>  !user_map.contains(user) & !item_map.contains(business)}

    // Make cold-start predictions!

    // Prediction for cold_start type1 (New user):
    val new_user = test_cs1.map{case (user, business, rate) => ((user, business), calcMean(item_map(business)))}

    // Prediction for cold_start type2 (New business)
    val new_business = test_cs2.map{case (user, business, rate) => ((user, business), calcMean(user_map(user)))}

    // Prediction for cold_start type3 (New business & user)
    val t_mean = train_ratings.map{case (user, business, rate) => rate}.mean()
    val new_both = test_cs3.map{case (user, business, rate) => ((user, business), t_mean)}

    // Put non-cold-start test data in proper form
    val toEval = test_given.map {
      case (user, business, rate) => (user, business)
    }

    // Form table so can iterate row by row and calculate needed item similarity
    val comp_table = toEval.map{case (user,business) => ((user, business), user_map(user))}.flatMap{case (key, value) => value.map(v => (key, v))}

    // Process the table entries to get item similarity
    val w_cossim = comp_table.map{case ((user,active_b),(passive_b,rate)) => ((user,active_b),(rate, calcSim(item_map(active_b), item_map(passive_b))))}

    // Collect the table rows related to 1 prediction (active user, active busienss) and make prediction
    val pred = w_cossim.groupByKey().map{case ((user,business),data) => ((user,business),makePred(data, calcMean(user_map(user)),calcMean(item_map(business)), t_mean))}

    // Create a combined prediction w/ cold-starts
    val union_pred = pred.union(new_user).union(new_business).union(new_both)

    // Combine with ground truth for evaluation
    val together = test_ratings.map {
      case (user, business, rate) => ((user, business), rate)
    }.join(union_pred)

    // Output to file
    val listed = union_pred.collect.sortBy{case (k,v) => k}

    val file = new File("./Pradeep_Lam_ItemBasedCF.txt")
    val fp = new BufferedWriter(new FileWriter(file))

    for(i <- listed.indices ){
        fp.write(s"${listed(i)._1._1},${listed(i)._1._2},${listed(i)._2}")

      if (i != listed.length - 1){
        fp.write("\n")
      }
    }

    fp.close()

    // Output to terminal
    val RMSE = math.sqrt(together.map {
      case ((user, business), (r1, r2)) =>
        val err = (r1 - r2)
        err * err
    }.mean())

    val grouped = together.map{case ((user, business), (r1, r2)) => calcGroup(r1,r2) -> 1}.reduceByKey(_+_).collect.toMap

    println(">=0 and <1: " + grouped(">=0 and <1"))
    println(">=1 and <2: " + grouped(">=1 and <2"))
    println(">=2 and <3: " + grouped(">=2 and <3"))
    println(">=3 and <4: " + grouped(">=3 and <4"))
    println(">=4: " + grouped(">=4"))

    println(s"Mean Squared Error = $RMSE")
    println(s"Time: ${(System.nanoTime - start_time)/1e9d}")
  }

}
