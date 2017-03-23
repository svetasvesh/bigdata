import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import SparkContext._

object Main {

	def main(args: Array[String]) {
		val appName = "SparkWordCount"
		val jars = List(SparkContext.jarOfObject(this).get)
		println(jars)
		val conf = new SparkConf().setAppName(appName).setJars(jars)
		val sc = new SparkContext(conf)
		if (args.length < 2) {
			println(args.mkString(","))
			println("ERROR. Please, specify input and output directories.")
		} else {
			val inputDir = args(0)
			val outputDir = args(1)
			println("Input directory: " + inputDir)
			println("Output directory: " + outputDir)
			run(sc, inputDir, outputDir)
		}
	}

	def run(sc: SparkContext, inputDir: String, outputDir: String) {
		val textFile = sc.textFile(inputDir + "/*")
	
		val data = textFile.flatMap{ line => line.split("\\W").filter(word => !word.isEmpty()).sliding(2).filter(x => x.length > 1).map(x => (x(0), x(1)))} 
			.groupByKey()
			.map( x => (x._1, x._2.groupBy(w => w).mapValues(word => word.size).map(x => (x._2, x._1)).max))
			.map(x => (x._2._1, (x._2._2, x._1)))
			.sortByKey(false)
			.first();
		
		var rdd = sc.parallelize(Seq(data))
			rdd.saveAsTextFile(outputDir);                         
	
	}	
}
















