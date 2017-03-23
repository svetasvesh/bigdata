import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import SparkContext._
import java.io.File
import scala.util.matching.Regex
import scala.io.Source

import java.io.BufferedReader
import java.io.InputStreamReader
import java.util.zip.GZIPInputStream
import java.io.FileInputStream


object Main {

	def main(args: Array[String]) {
		val appName = "SparkSpec"
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

	val GLUSTERFS_MOUNT_PATH = "/mnt/root"
	val FILENAME_PATTERN = "([0-9]{5})([dijkw])([0-9]{4})\\.txt\\.gz".r
	val LINE_PATTERN = "([0-9]{4} [0-9]{2} [0-9]{2} [0-9]{2} [0-9]{2})(.*)".r
	val VARIABLE_NAMES = Array("d", "i", "j", "k", "w")

	def toGlusterfsPath(path: String): String = path.replace(GLUSTERFS_MOUNT_PATH, "")
	def toGlusterfsPath(file: File): String = toGlusterfsPath(file.getAbsolutePath())

	def toFloatArray(line: String) = line.split("\\s+").filter(!_.isEmpty).map(word => word.toFloat)
	def fmtArr(x: Array[Float]) = "[" + x.mkString(", ") + "]"

	def swap[A](l: List[A], x: Int) = {
	  val (l1,rest) = l.splitAt(x)
	//  val (l2,l3) = rest.splitAt(rest.length-x)
	   rest ::: l1
	}


	def run(sc: SparkContext, inputDir: String, outputDir: String) {
		val input = new File (inputDir)
		val pathNames = input.list()
		val Names = pathNames.toList
			.map(x => x match {case FILENAME_PATTERN(number, variable, year) => (number, variable, year)})  //вытащили номер, переменную, год для всех файлов
			.map(x => (x._1 + "-" + x._3, x._2)).groupBy(x => x._1)  //загнали в хэш, сгруппировав по номеру
			.map(x => (x._1, x._2, x._2.size))  
			.filter(x => (x._3 == 5)) //получили пятерки файлов станций
			.map(x => x._2) //убираем количество файлов. оставляем (list[number, variable])
			.flatMap(line => line.map(x => (x._1, x._1.replace("-", x._2)+".txt.gz", x._2))) // получаем (number, filename, variable)		


			val data = Names.map( x => {   
				sc.textFile(toGlusterfsPath(inputDir) + "/" + x._2) 
				.filter(line => LINE_PATTERN.pattern.matcher(line).matches) //отбрасываем строки-заголовки
				.map(line => line match {case LINE_PATTERN(date, spectres) => (date, spectres,x._1, x._3) } ) //собираем (date, spectres, number, variable)
				
			} ).reduce(_ union _)	
				.groupBy(x => x._3) //группируем по номеру станции
				.map(station => station._2.groupBy(x => x._1)) //в станциях группируем по дате
				.flatMap(station => station  //для каждой станции...
					.map(date => (date._1, date._2, date._2.size)) //считаем количество данных для каждой даты
					.filter(date => (date._3 >= 5))  //отбираем пятерки
					.map(date => (date._1, date._2
						.toList.distinct
						.map(x => (x._4, fmtArr(toFloatArray(x._2)) ) ) //(date, (i = , j = , k = , w = , d = ))
						.toList.sortBy(x => x._1)  //сортируем по переменной
						.map(x => x._1 + "=" + x._2) //variable = spectres
					))
					.map(date => (date._1, swap(date._2, 1)))	
					.map(date => (date._1, date._2
						.reduce ((x,y) => (x + "," + y)) //склеиваем все переменные в одну строку
				
					)) 
				)
				.sortByKey() 
				.map(date => date._1 + "\t" +  "[" + date._2 + "]" )		

			data.saveAsTextFile(outputDir);		

	}

}

//(1 to M) flatMap (x => (1 to N) map (y => (x, y)))
		

