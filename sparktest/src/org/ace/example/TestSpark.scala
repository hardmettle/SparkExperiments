
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark._
import org.apache.spark.rdd.PairRDDFunctions
/**
 * Created by harsh on 1/8/14.
 */

object TestSpark {
  val sc = new SparkContext()
  val logRdd = sc.textFile("/workspaces/harsh/log_example.txt")
  val logregex = """([\w\s]+\d+\s\d+\:\d+\:\d+\s\w+\s\d+)\s\:\s\[(\w+)\]\s\:\s(.*)""".r
  def getCategoryCount(line:String):(String,Int)={
    line match {
      case logregex(dt,cat,msg) =>   (cat.toString,1)
    }

  }
  //Wed Jul 30 20:00:18 IST 2014 : [INFO] : UI HI !! ITS AN EVEN TEST MESSAGE,Hi its and even test detail 228
  def main(args: Array[String])= {
    val cat_counts = logRdd.map(l=>getCategoryCount(l)).reduceByKey(_+_)
    cat_counts.foreach(println)
  }
}
