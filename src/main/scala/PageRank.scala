import org.apache.spark._
import org.apache.hadoop.fs._
import collection.mutable.HashMap
import scala.util.control.Breaks._

import org.apache.spark.{AccumulableParam, SparkConf}
import org.apache.spark.serializer.JavaSerializer
import scala.collection.mutable.{ HashMap => MutableHashMap }
import org.apache.spark.Accumulable
import org.apache.spark._
import org.apache.spark.SparkContext._

class MapAccumulator extends AccumulableParam[MutableHashMap[String, Int], (String, Int)] {

	private var accumulator: Accumulable[MutableHashMap[String, Int], (String, Int)] = _

	def addAccumulator(acc: MutableHashMap[String, Int], elem: (String, Int)): MutableHashMap[String, Int] = {
		val (k1, v1) = elem
		acc += acc.find(_._1 == k1).map {
			case (k2, v2) => k2 -> (v1 + v2)
		}.getOrElse(elem)

		acc
	}

	def addInPlace(acc1: MutableHashMap[String, Int], acc2: MutableHashMap[String, Int]): MutableHashMap[String, Int] = {
		acc2.foreach(elem => addAccumulator(acc1, elem))
		acc1
	}

	def zero(initialValue: MutableHashMap[String, Int]): MutableHashMap[String, Int] = {
		val ser = new JavaSerializer(new SparkConf(false)).newInstance()
		val copy = ser.deserialize[MutableHashMap[String, Int]](ser.serialize(initialValue))
		copy.clear()
		copy
	}

	def getInstance(sc: SparkContext): Accumulable[MutableHashMap[String, Int], (String, Int)] = {
		if (accumulator == null) {
			synchronized {
				if (accumulator == null) {
					 accumulator = sc.accumulable(MutableHashMap.empty[String, Int])(new MapAccumulator)
				}
			}
		}
		accumulator
	}

}

class MapAccumulatorDouble extends AccumulableParam[MutableHashMap[String, Double], (String, Double)] {

	private var accumulator: Accumulable[MutableHashMap[String, Double], (String, Double)] = _

	def addAccumulator(acc: MutableHashMap[String, Double], elem: (String, Double)): MutableHashMap[String, Double] = {
		val (k1, v1) = elem
		acc += acc.find(_._1 == k1).map {
			case (k2, v2) => k2 -> (v1 + v2)
		}.getOrElse(elem)

		acc
	}

	def addInPlace(acc1: MutableHashMap[String, Double], acc2: MutableHashMap[String, Double]): MutableHashMap[String, Double] = {
		acc2.foreach(elem => addAccumulator(acc1, elem))
		acc1
	}

	def zero(initialValue: MutableHashMap[String, Double]): MutableHashMap[String, Double] = {
		val ser = new JavaSerializer(new SparkConf(false)).newInstance()
		val copy = ser.deserialize[MutableHashMap[String, Double]](ser.serialize(initialValue))
		copy.clear()
		copy
	}

	def getInstance(sc: SparkContext): Accumulable[MutableHashMap[String, Double], (String, Double)] = {
		if (accumulator == null) {
			synchronized {
				if (accumulator == null) {
					 accumulator = sc.accumulable(MutableHashMap.empty[String, Double])(new MapAccumulatorDouble)
				}
			}
		}
		accumulator
	}

}

object PageRank {
	def main(args: Array[String]) {
		val filePath = args(0)
		val outputPath = args(1)
		var iteration = 20000 //max iteration
		val dpFactor = 0.85
		var isStopIter = false

		val conf = new SparkConf().setAppName("PageRank Application")
		val sc = new SparkContext(conf)
		val mc = new MapAccumulator()
		val acc = mc.getInstance(sc)
		val mcDouble = new MapAccumulatorDouble()
		val accPR = mcDouble.getInstance(sc)
		var mapForPR = new MutableHashMap[String, Double]()

		// Cleanup output dir
		val hadoopConf = sc.hadoopConfiguration
		var hdfs = FileSystem.get(hadoopConf)
		try { hdfs.delete(new Path(outputPath), true) } catch { case _ : Throwable => { } }

		//var titleMap = new MutableHashMap[String, Int]()
		//var prMap = new MutableHashMap[String, Int]() //for store page rank after iteration

		val patternTitle = "<title>(.+?)</title>".r
		val patternLink = "\\[\\[(.+?)([\\|#]|\\]\\])".r
		// Read input file
		val lines = sc.textFile(filePath, sc.defaultParallelism)
		var tileLinks = lines.map(line => {
			//val titleMatchOpt = patternTitle.findFirstIn(line) //error handle if no title
			//val titleMatch = titleMatchOpt.getOrElse(" ") //convert option to string
			val titleMatch = patternTitle.findFirstMatchIn(line).get
			val titleMatchDecoded = unescape(titleMatch.group(1))
			val linkMatch =
				patternLink.findAllIn(line).matchData.map(_.group(1)).toList.map(w => unescape(w)).map(_.capitalize)
			//println("titleMatch: " + titleMatchDecoded)
			//println("linkMatch: " + linkMatch)
			//filter invalid links!

			//titleMap += (titleMatchDecoded -> 1) //add title to the map => invalid!!! WTF
			acc += (titleMatchDecoded -> 1)
			(titleMatchDecoded, linkMatch)
		})

		tileLinks.count();
		val mapp = acc.value;
		//println("Test: " + acc.value)

		val tlList = tileLinks.map(s => (s._1, s._2.filter(mapp.contains(_))))
		//tlList.foreach(s => println(s._1 + "\t" + s._2))

		val nodeNumber = tlList.count()
		//println("nodeNumber: " + nodeNumber)
		var pageRanks = tlList.mapValues(value => 1.0 / nodeNumber) //give PR initial value
		//Start Iteration
		var i = 1
		while ((i < iteration) && !isStopIter) {
			
			val dangling = sc.accumulator(0.0)
			val err = sc.accumulator(0.0)

			val contribs = tlList.join(pageRanks).values.flatMap {
				case (urls, rank) => {
					val outlinkNum = urls.size
					if (outlinkNum == 0) {
						dangling += rank
						List()
					} else {
						urls.map(url => (url, rank / outlinkNum))
					}
				}
			}

			val all = tlList.mapValues(value => 0.0)
			val contribsAll = contribs ++ all

			//contribsAll.foreach(s => println(s._1 + "\t" + s._2))
			contribsAll.count()
			val danglingValue = dangling.value
			//println("### danglingValue: " + danglingValue)
			pageRanks = contribsAll.reduceByKey(_ + _).mapValues[Double](p =>
				(1-dpFactor) * (1.0 / nodeNumber) + dpFactor * (danglingValue / nodeNumber + p)
			)
			println("------------------------------" + i + "---------------------------------")
			//pageRanks.foreach(s => println(s._1 + " - " + s._2))

			//compute err
			pageRanks.foreach(s => {
				var prOld = 0.0
				if (mapForPR.contains(s._1)) {
					prOld = mapForPR(s._1)
				}
				//println("ABS: " + math.abs(s._2 - prOld) + "  prOld" + prOld)
				err += math.abs(s._2 - prOld)
				accPR += (s._1 -> (s._2 - prOld))
			})

			pageRanks.count()
			mapForPR = accPR.value
			//mcDouble.zero(accPR.value)
			//println("Test: " + mapForPR)

			val errValue = err.value
			println("@@@@@======= err ====== " + errValue)
			if((errValue * 10000) < 10) {
				isStopIter = true
			}
			//pageRanks.foreach(s => 	(s._1 + "\t" + s._2))
			//pageRanks = sc.parallelize(pageRanks.collect, (sc.defaultParallelism * 3))
			i = i+1
		}

		//sorting and write result
		pageRanks.sortBy{ case(title, prValue) => (-prValue, title) }.map(r => (r._1 + "\t" + r._2)).saveAsTextFile(outputPath)
		//result.foreach(s => println(s._1 + "\t" + s._2))
		//result.foreach(s => println(s))
		//result.count()
		// Action branch! Add cache() to avoid re-computation
		//res = res.cache

		// Output: count() and saveAsTextFile()
		//result.saveAsTextFile(outputPath)
		sc.stop
	}

	def unescape(text:String):String = {
		def recUnescape(textList:List[Char],acc:String,escapeFlag:Boolean):String= {
			textList match {
			case Nil => acc
			case '&'::tail => recUnescape(tail,acc,true)
			case ';'::tail if (escapeFlag) => recUnescape(tail,acc,false)
			case 'a'::'m'::'p'::tail if (escapeFlag) => recUnescape(tail,acc+"&",true)
			case 'q'::'u'::'o'::'t'::tail if (escapeFlag) => recUnescape(tail,acc+"\"",true)
			case 'a'::'p'::'o'::'s'::tail if (escapeFlag) => recUnescape(tail,acc+"'",true)
			case 'l'::'t'::tail if (escapeFlag) => recUnescape(tail,acc+"<",true)
			case 'g'::'t'::tail if (escapeFlag) => recUnescape(tail,acc+">",true)
			case x::tail => recUnescape(tail,acc+x,true)
			case _ => acc
			}
		}
		recUnescape(text.toList,"",false)
	}
}