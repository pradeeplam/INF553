import org.apache.spark.sql.SparkSession
import org.apache.spark.HashPartitioner
import java.io._

object Task2 {
  def main(args: Array[String]): Unit = {

    var  ipath = ""
    var opath = ""

    if (args.length != 2){
      ipath = "./survey_results_public.csv"
      opath = "./Task2.csv"
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

    // Easier to scrub a DataFrame
    val scrubbed = data.where(data.col("Salary").rlike("[1-9]+") && data.col("Salary") =!= "NA")
    // Convert to RDD for purpose of this task (Could have done Task 1 similarly)
    val relevant = scrubbed.select("Country").rdd

    // Without partition
    val num_p1 = relevant.partitions.size
    val num_e1 = relevant.mapPartitionsWithIndex((index, elements) => Iterator(Seq(index,elements.size))).collect()

    val start1 = System.nanoTime
    val dummy1 = relevant.map(entry => (entry, 1)).reduceByKey(_+_).collect() // Basically how you'd do Task 1 w/ just RDD
    val elapsed1 = (System.nanoTime - start1)/1000000

    // With partition
    val part = relevant.map(row => (row,1)).partitionBy(new HashPartitioner(2))
    val num_p2 = part.partitions.size
    val num_e2 = part.mapPartitionsWithIndex((index, elements) => Iterator(Seq(index,elements.size))).collect()

    val start2 = System.nanoTime
    val dummy2 = part.map(entry => (entry, 1)).reduceByKey(_+_).collect()
    val elapsed2 = (System.nanoTime - start2)/1000000

    // Output to file
    val file = new File(opath)
    val fp = new BufferedWriter(new FileWriter(file))

    // First line
    fp.write("standard,")
    for(i <- num_e1.indices){
      fp.write(num_e1(i)(1).toString+",")
    }
    fp.write(elapsed1.toString)
    fp.write("\n")

    // Second line
    fp.write("partition,")
    for(i <- num_e2.indices){
      fp.write(num_e2(i)(1).toString+",")
    }
    fp.write(elapsed2.toString)

    fp.close()
  }
}