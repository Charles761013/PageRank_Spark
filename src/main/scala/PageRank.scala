import org.apache.spark._
import org.apache.hadoop.fs._
import collection.mutable.HashMap
import scala.util.control.Breaks._
import scala.collection.mutable.ListBuffer

import org.apache.spark.{AccumulableParam, SparkConf}
import org.apache.spark.serializer.JavaSerializer
import scala.collection.mutable.{ HashMap => MutableHashMap }
import org.apache.spark.Accumulable
import org.apache.spark._
import org.apache.spark.SparkContext._


object PageRank {
	def main(args: Array[String]) {
		val filePath = args(0)
		val outputPath = args(1)
		var iteration = 20000 //max iteration
		val dpFactor = 0.85
		var isStopIter = false

		val conf = new SparkConf().setAppName("PageRank Application") //.set("spark.executor.memory", "5g")
		val sc = new SparkContext(conf)

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

			(titleMatchDecoded, linkMatch)
		}).cache()

		val validTitle = tileLinks.keys.keyBy(k => k)
		val checkTitle = tileLinks.flatMap {
			case (t, links) => {
				links.map(l => (l, t))
			}
		}

		val tlListTemp = validTitle.leftOuterJoin(checkTitle).filter{
			case (k, (v1, v2)) =>  v2 != None
		}.map({
				case (l, (t1, Some(matchingTitle))) => (matchingTitle, l)
				case (l, (t1, None)) => (t1, "")
		}).groupByKey.map(s => (s._1, s._2.toList)).cache

		val dummy = validTitle.keys.subtract(tlListTemp.keys).map(p => (p, List[String]())).cache
		val tlList = tlListTemp ++ dummy

		//tlList.foreach(s => println(s._1 + "\t" + s._2))

		val nodeNumber = tlList.count()
		var pageRanks = tlList.mapValues(value => 1.0 / nodeNumber) //give PR initial value
		var prRecord = tlList.mapValues(value => 0.0).cache()

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
			}.cache()

			val all = tlList.mapValues(value => 0.0).cache()
			var contribsAll = contribs ++ all
			contribsAll = contribsAll.cache()

			//contribsAll.foreach(s => println(s._1 + "\t" + s._2))
			contribsAll.count()
			val danglingValue = dangling.value
			pageRanks = contribsAll.reduceByKey(_ + _).mapValues[Double](p =>
				(1-dpFactor) * (1.0 / nodeNumber) + dpFactor * (danglingValue / nodeNumber + p)
			).cache()
			println("\n\n\n\n\n=============" + i + "=============")
			//pageRanks.foreach(s => println(s._1 + " - " + s._2))

			//compute err
			pageRanks.join(prRecord).values.foreach {
				case (pr1, pr2) => {
					err += math.abs(pr1 - pr2)

				}
			}
			prRecord = pageRanks

			val errValue = err.value
			println("\n\n\n\n\n----err----" + errValue)
			if((errValue * 10000) < 10) {
				isStopIter = true
			}

			i = i + 1
		}

		//sorting and write result
		pageRanks.sortBy{ case(title, prValue) => (-prValue, title) }.map(r => (r._1 + "\t" + r._2)).saveAsTextFile(outputPath)

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