package questions

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import scala.math._

import org.apache.spark.graphx._
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph

import scala.util.matching._


/** GeoProcessor provides functionalites to 
* process country/city/location data.
* We are using data from http://download.geonames.org/export/dump/
* which is licensed under creative commons 3.0 http://creativecommons.org/licenses/by/3.0/
*
* @param sc reference to SparkContext 
* @param filePath path to file that should be modified
*/
class GeoProcessor(sc: SparkContext, filePath:String) {

    //read the file and create an RDD
    //DO NOT EDIT
    val file = sc.textFile(filePath)

    /** filterData removes unnecessary fields and splits the data so
    * that the RDD looks like RDD(Array("<name>","<countryCode>","<dem>"),...))
    * Fields to include:
    *   - name
    *   - countryCode
    *   - dem (digital elevation model)
    *
    * @return RDD containing filtered location data. There should be an Array for each location
    */
    def filterData(data: RDD[String]): RDD[Array[String]] = {
        /* hint: you can first split each line into an array.
        * Columns are separated by tab ('\t') character. 
        * Finally you should take the appropriate fields.
        * Function zipWithIndex might be useful.
        */

        data.map(line => line.split('\t'))
          .map(line => Array(line(1),line(8),line(16)))
    }


    /** filterElevation is used to filter to given countryCode
    * and return RDD containing only elevation(dem) information
    *
    * @param countryCode code e.g(AD)
    * @param data an RDD containing multiple Array[<name>, <countryCode>, <dem>]
    * @return RDD containing only elevation information
    */
    def filterElevation(countryCode: String,data: RDD[Array[String]]): RDD[Int] = {
        val myfillter3 = data.filter(x =>x(1).equals(countryCode))
        val myresult = myfillter3.map(x => x(2).toInt)

	return myresult
    }



    /** elevationAverage calculates the elevation(dem) average
    * to specific dataset.
    *
    * @param data: RDD containing only elevation information
    * @return The average elevation
    */
    def elevationAverage(data: RDD[Int]): Double = {
        val average = data.sum / data.count
	
	return average
    }

    /** mostCommonWords calculates what is the most common 
    * word in place names and returns an RDD[(String,Int)]
    * You can assume that words are separated by a single space ' '. 
    *
    * @param data an RDD containing multiple Array[<name>, <countryCode>, <dem>]
    * @return RDD[(String,Int)] where string is the word and Int number of 
    * occurrences. RDD should be in descending order (sorted by number of occurrences).
    * e.g ("hotel", 234), ("airport", 120), ("new", 12)
    */
    def mostCommonWords(data: RDD[Array[String]]): RDD[(String,Int)] = {
        val mywords = data.map(x => x(0))
	    val splittedmywords = mywords.flatMap(x => x.split("\\s+")).map((_,1))
	    val myreduce = splittedmywords.reduceByKey(_+_).sortBy(_._2,false)

        return myreduce
    }

    /** mostCommonCountry tells which country has the most
    * entries in geolocation data. The correct name for specific
    * countrycode can be found from countrycodes.csv.
    *
    * @param data filtered geoLocation data
    * @param path to countrycode.csv file
    * @return most common country as String e.g Finland or empty string "" if countrycodes.csv
    *         doesn't have that entry.
    */
    def mostCommonCountry(data: RDD[Array[String]], path: String): String = {
        val code = data.map(l => (l(1),1))
          .reduceByKey(_+_)
          .map(l => l.swap)
          .sortByKey(false)
          .map(l => l.swap)
          .first._1
        sc.textFile(path)
          .map(l => l.split(","))
          .filter(l => l(1).contains(code))
          .map(_.head)
          .collect()
          .mkString
    }

//
    /**
    * How many hotels are within 10 km (<=10000.0) from
    * given latitude and longitude?
    * https://en.wikipedia.org/wiki/Haversine_formula
    * earth radius is 6371e3 meters.
    *
    * Location is a hotel if the name contains the word 'hotel'.
    * Don't use feature code field!
    *
    * Important
    *   if you want to use helper functions, use variables as
    *   functions, e.g
    *   val distance = (a: Double) => {...}
    *
    * @param lat latitude as Double
    * @param long longitude as Double
    * @return number of hotels in area
    */
    def hotelsInArea(lat: Double, long: Double): Int = {
        val distance = (lat1: Double, long1: Double,
          lat2: Double, long2: Double) => {
            val earthRadius = 6371e3
            val dLat = Math.toRadians(lat2-lat1)
            val dLng = Math.toRadians(long2-long1)
            val sindLat = Math.sin(dLat / 2)
            val sindLong = Math.sin(dLng / 2)
            val a = Math.pow(sindLat, 2) + Math.pow(sindLong, 2) *
              Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
            val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a))
            earthRadius * c
        }
        file.map(line => line.split('\t'))
          .map(line => Array(line(1),line(4),line(5)))
          .filter(line => line(0).toLowerCase.contains("hotel"))
          .filter(line => distance(lat, long, line(1).toDouble, line(2).toDouble) < 10)
          .count
          .toInt
    }

    //GraphX exercises

    /**
    * Load FourSquare social graph data, create a
    * graphx graph and return it.
    * Use user id as vertex id and vertex attribute.
    * Use number of unique connections between users as edge weight.
    * E.g
    * ---------------------
    * | user_id | dest_id |
    * ---------------------
    * |    1    |    2    |
    * |    1    |    2    |
    * |    2    |    1    |
    * |    1    |    3    |
    * |    2    |    3    |
    * ---------------------
    *         || ||
    *         || ||
    *         \   /
    *          \ /
    *           +
    *
    *         _ 3 _
    *         /' '\
    *        (1)  (1)
    *        /      \
    *       1--(2)--->2
    *        \       /
    *         \-(1)-/
    *
    * Hints:
    *  - Regex is extremely useful when parsing the data in this case.
    *  - http://spark.apache.org/docs/1.6.1/graphx-programming-guide.html
    *
    * @param path to file. You can find the dataset
    *  from the resources folder
    * @return graphx graph
    *
    */
    def loadSocial(path: String): Graph[Int,Int] = {
        val file = sc.textFile(path)
	val regex = new Regex("""([0-9]+)""")
	var afterregex = file.map(x=>regex.findAllIn(x).toArray)
	var myfilter = afterregex.filter(x=>x.length ==2)
	var mytry = myfilter.map(x=>(x(0),x(1)))
	var one  = mytry.map(x=>(x,1))
	val myreduce = one.reduceByKey(_+_)

	var flat = mytry.map(x=>x._1+","+x._2)
 	val textFlatMapped = flat.flatMap(_.split(","))
	var distext=textFlatMapped.distinct
	var vertextable = distext.map(x=>(x.toLong,x.toInt))
	val vertexRDD: RDD[(Long,Int)] = vertextable

	var edgestable = myreduce.map(x=>(x._1._1.toLong,x._1._2.toLong,x._2))
	val edgeRDD: RDD[Edge[Int]] = edgestable.map(x=>Edge(x._1,x._2,x._3))
 	val graph: Graph[Int, Int] = Graph(vertexRDD, edgeRDD)
        
        return graph
    }

    /**
    * Which user has the most outward connections.
    *
    * @param graph graphx graph containing the data
    * @return vertex_id as Int
    */
    def mostActiveUser(graph: Graph[Int,Int]): Int = {
        graph.outDegrees
          .map(line => line.swap)
          .sortByKey(false)
          .map(l => l.swap)
          .first._1
          .toInt
    }

    /**
    * Which user has the highest pageRank.
    * https://en.wikipedia.org/wiki/PageRank
    *
    * @param graph graphx graph containing the data
    * @return user with highest pageRank
    */
   def pageRankHighest(graph: Graph[Int,Int]): Int = {
        graph.pageRank(0.001)
          .vertices
          .map(line => line.swap)
          .sortByKey(false)
          .map(l => l.swap)
          .first._1
          .toInt
    } 
}
/**
*
*  Change the student id
*/
object GeoProcessor {
    val studentId = "584830"
}