import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.io._

object Task3 {
  def main(args: Array[String]): Unit = {

    var  ipath = ""
    var opath = ""

    if (args.length != 2){
      ipath = "./survey_results_public.csv"
      opath = "./Task3.csv"
    }
    else{
      ipath= args(0)
      opath = args(1)
    }

    // Create a new spark session
    val spark = SparkSession.builder
      .appName("Task1")
      .master("local[2]")
      .config("spark.name.config.option","some-value")
      .getOrCreate()

    // Reading input data from file
    val data = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(ipath)

    // Relevant values
    val rcol = Seq("Salary", "SalaryType", "Country")

    // Easier to scrub a DataFrame
    val scrubbed = data.where(data.col("Salary").rlike("[1-9]") && data.col("Salary") =!= "NA")

    // Only dealing with limited data
    val relevant = scrubbed.select(rcol.map(c => scrubbed.col(c)):_*)

    // Where the magic happens

    val corrected1 = relevant.withColumn("Salary", regexp_replace(relevant("Salary"), "\\,", ""))
    val corrected2 = corrected1.withColumn("Salary", corrected1("Salary").cast("Double"))


    // Standardize column probably using UDF
    def standardize = udf((salary: Double, stype: String) => {
      if(stype.toLowerCase == "weekly"){
        salary*52
      }
      else if(stype.toLowerCase == "monthly"){
        salary*12
      }
      else{
        salary
      }
    })

    val corrected3 = corrected2.withColumn("Salary", standardize(corrected2("Salary"),corrected2("SalaryType")))

    // Calculate stuff
    val count_val = corrected3.groupBy("Country").count().orderBy("Country").collect()
    val min_val = corrected3.groupBy("Country").min("Salary").orderBy("Country").collect()
    val max_val = corrected3.groupBy("Country").max("Salary").orderBy("Country").collect()
    val avg_val = corrected3.groupBy("Country").mean("Salary").orderBy("Country").collect()

    // Output to file
    val file = new File(opath)
    val fp = new BufferedWriter(new FileWriter(file))


    for(i <- count_val.indices ){
      if(count_val(i)(0).toString.contains(",")){
        fp.write("\"" + count_val(i)(0) + "\"" + "," +  count_val(i)(1) + "," + min_val(i)(1).toString.toDouble.toInt + "," + max_val(i)(1).toString.toDouble.toInt + "," + Math.ceil(100 * avg_val(i)(1).toString.toDouble)/100.0)
      }
      else{
        fp.write(count_val(i)(0) + "," +  count_val(i)(1) + "," + min_val(i)(1).toString.toDouble.toInt + "," + max_val(i)(1).toString.toDouble.toInt + "," + Math.ceil(100 * avg_val(i)(1).toString.toDouble)/100.0)
      }

      if (i != count_val.length - 1){
        fp.write("\n")
      }
    }

    fp.close()

  }
}