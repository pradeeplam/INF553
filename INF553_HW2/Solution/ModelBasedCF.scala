
import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating

object ModelBasedCF {

  def calcMean( data: Iterable[(Int,Double)], t_mean:Double) = {
    val ratings = data.map(x => x._2)

    if (ratings.size == 1)
      0.5 * t_mean + (0.5 * ratings.sum) // Gotta un-bias if only a single item
    else
      ratings.sum / ratings.size

  }

  def calcGroup(r1 : Double, r2: Double) = {
    val d = math.abs(r1 - r2)

    if (d >= 0 & d < 1)
      ">=0 and <1"
    else if (d >=1 & d < 2)
      ">=1 and <2"
    else if (d >=2 & d < 3)
      ">=2 and <3"
    else if (d >=3 & d < 4)
      ">=3 and <4"
    else
      ">=4"
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

    val train_data = spark_context.textFile("./train_review.csv").filter(row => row != "user_id,business_id,stars")
    val test_data = spark_context.textFile("./test_review.csv").filter(row => row != "user_id,business_id,stars")
    val unionized = train_data.union(test_data)

    /*
    Below is very inefficient! And def doesn't scale well.
    But... At this point in time, I don't see a better option.
    Autogen generates a long key as opposed to an int.
     */

    // Get unique ids
    val uid = unionized.map(_.split(',')(0)).distinct().collect.toList
    val bid = unionized.map(_.split(',')(1)).distinct().collect.toList

    // Create unique ids
    val uid_asint = 0 to (uid.size - 1) toArray
    val bid_asint = 0 to (bid.size - 1) toArray

    // Create a mapping using the ids
    val uid_dict = (uid zip uid_asint).toMap
    val bid_dict = (bid zip bid_asint).toMap

    // Create an inverse mapping
    val uid_inv = uid_dict.map(_.swap)
    val bid_inv = bid_dict.map(_.swap)

    // Each line is split into 3 element array & Array of 3 elements elements is mapped to a Rating object
    val train_ratings = train_data.map(_.split(',') match {
      case Array(user, business, rate) => Rating(uid_dict(user), bid_dict(business), rate.toDouble)
    })

    val test_ratings = test_data.map(_.split(',') match {
      case Array(user, business, rate) => Rating(uid_dict(user), bid_dict(business), rate.toDouble)
    })


    // Build the recommendation model using ALS

    val rank = 2 // Number of latent var
    val numIterations = 17 // Number of times to run u-p regressions
    val lambda = 0.25
    val blocks = -1
    val seed = 1 // For consistency

    // Train the model
    val model = ALS.train(train_ratings, rank, numIterations, lambda, blocks, seed)


    /*
    Running the model on raw test data will produce shitty results.
    This is because there are test cases not accounted for by the model.
    These cases may come in the form of:
    1. Cold-Start users
    2. Cold-Start businesses
    3. Cold-Start users & businesses
    It would be smart to filter out these cases and manually take care of them.
     */

    // Inefficient as hell but w/e..Spent enough time trying to make distributed
    // Create hash maps to lists of items/users for user/item
    val item_map = train_ratings.map{case Rating(user,business,rate) => (business,(user,rate))}.groupByKey().collectAsMap()
    val user_map = train_ratings.map{case Rating(user,business,rate) => (user,(business,rate))}.groupByKey().collectAsMap()


    // Filter test data
    val test_given = test_ratings.filter{case Rating(user, business, rate) =>  user_map.contains(user) & item_map.contains(business)}

    val test_cs1 = test_ratings.filter{case Rating(user, business, rate) =>  !user_map.contains(user) & item_map.contains(business)}
    val test_cs2 = test_ratings.filter{case Rating(user, business, rate) =>  user_map.contains(user) & !item_map.contains(business)}
    val test_cs3 = test_ratings.filter{case Rating(user, business, rate) =>  !user_map.contains(user) & !item_map.contains(business)}


    // Evaluate the model on rating data
    val toEval = test_given.map {
      case Rating(user, business, rate) => (user, business)
    }

    // Predict spits out a Rating object which is mapped to a touple of touple and double
    val pred  = model.predict(toEval).map {
        case Rating(user, business, rate) => ((user, business), rate)
    }

    // Predict for cold-start data
    val t_mean = train_ratings.map{case Rating(user, business, rate) => rate}.mean()

    // Prediction for cold_start type1 (New user):
    val new_user = test_cs1.map{case Rating(user, business, rate) => ((user, business), calcMean(item_map(business),t_mean))}

    // Prediction for cold_start type2 (New business)
    val new_business = test_cs2.map{case Rating(user, business, rate) => ((user, business), calcMean(user_map(user),t_mean))}

    // Prediction for cold_start type3 (New business & user)
    val new_both = test_cs3.map{case Rating(user, business, rate) => ((user, business), t_mean)}

    // Create a combined prediction
    val union_pred = pred.union(new_user).union(new_business).union(new_both)

    // Output to file
    val listed = union_pred.collect.map{case ((user,business),rate) => ((uid_inv(user),bid_inv(business)),rate)}.sortBy{case (k,v) => k}

    val file = new File("./Pradeep_Lam_ModelBasedCF.txt")
    val fp = new BufferedWriter(new FileWriter(file))

    for(i <- listed.indices ){
      fp.write(s"${listed(i)._1._1},${listed(i)._1._2},${listed(i)._2}")

      if (i != listed.length - 1){
        fp.write("\n")
      }
    }

    fp.close()

    // Ratings object is mapped to touple of tuple of tuple and double
    val together = test_ratings.map {
      case Rating(user, business, rate) => ((user, business), rate)
    }.join(union_pred)

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

