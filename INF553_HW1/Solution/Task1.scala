
import org.apache.spark.sql.SparkSession
import java.io._

object Task1 {
  def main(args: Array[String]): Unit = {

    var  ipath = ""
    var opath = ""

    if (args.length != 2){
      ipath = "./survey_results_public.csv"
      opath = "./Task1.csv"
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

    val scrubbed = data.where(data.col("Salary").rlike("[1-9]+") && data.col("Salary") =!= "NA")

    val output = scrubbed.groupBy("Country").count().sort("Country")

    val total = scrubbed.count()

    val listed = output.collect()

    // Output to file
    val file = new File(opath)
    val fp = new BufferedWriter(new FileWriter(file))

    fp.write("Total" + "," + total + "\n")

    for(i <- listed.indices ){
      // Edge case w/ , in name
      if(listed(i)(0).toString.contains(",")){
        fp.write("\"" + listed(i)(0) + "\"" + "," + listed(i)(1))
      }
      else {
        fp.write(listed(i)(0) + "," + listed(i)(1))
      }
      if (i != listed.length - 1){
        fp.write("\n")
      }
    }

    fp.close()
  }
}
