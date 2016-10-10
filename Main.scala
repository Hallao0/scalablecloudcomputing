package questions

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Main {
    def main(args: Array[String]) = {
	
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)

        val conf = new SparkConf().setAppName("local-cluster").setMaster("local")
        val sc = new SparkContext(conf)

        val path = getClass().getResource("/allCountries.txt").toString
        val processor = new GeoProcessor(sc,path)

        //example for printing
        //val filtered = processor.filterData(processor.file)

        //filtered.take(5).foreach(x => println(x.mkString(" ")))

        //processor.filterElevation("FI", filtered).take(10).foreach(x => print(x + " "))

        //stop spark
        sc.stop()
    }
}
