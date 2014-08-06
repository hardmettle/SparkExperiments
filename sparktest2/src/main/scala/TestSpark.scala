/**
 * Created by harsh on 1/8/14.
 */

import java.io.{File, PrintWriter}

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark._


object TestSpark {
  val writer = new PrintWriter(new File("/workspaces/harsh/log_example_op.txt"))
  val sparkConf = new SparkConf().setAppName("Sparklogtest").setMaster("local[4]")
  val sc = new SparkContext(sparkConf)
  /* No parallelism and partition - 51
  * 10,5 -> 60 ms
  * 10,50 -> 51 ms
  * 20,_ ->
  * 700 - 49 ms
  * 5 - 51 ms
  * 50 - 61 ms
  * 250 - 59 ms
  * */
  //System.setProperty("spark.default.parallelism","4")
    val logRdd = sc.textFile("/workspaces/harsh/test.txt")

  val logregex = """([\w\s]+\d+\s\d+\:\d+\:\d+\s\w+\s\d+)\s\:\s\[(\w+)\]\s\:\s(.*)""".r
  def getCategoryCount(line:String):(String,Int)={
    line match {
      case logregex(dt,cat,msg) =>   (cat.toString,1)
    }
  }
  def getComponentErrorList(line:String):(String,String)={
    var component = ""
    var detail = ""
    val msg_reg = """^(\w+\s)(.*)""".r
    line match {
      case logregex(dt,cat,msg)=>   {
        msg.toString match{
          case msg_reg(c,dtl) => {component = c;detail= dtl}
        }
        (component,detail)
      }
    }
  }
  //Wed Jul 30 20:00:18 IST 2014 : [INFO] : UI HI !! ITS AN EVEN TEST MESSAGE,Hi its and even test detail 228
  def main(args: Array[String])= {
    val starttime = (System.currentTimeMillis)
    val cat_counts = logRdd.map(l=>getCategoryCount(l)).reduceByKey(_+_,1)
    val endtime = System.currentTimeMillis
    println(s"Total time taken for run : ${endtime-starttime} milliseconds")
    //System.exit(255)
    //println(cat_counts.collect()(0)._1)
    //cat_counts.foreach(println)
    /*This is the second part for collection of component and corresponding error sequence
    val filtered_logrdd = logRdd.filter(l=>l.contains("[ERROR]"))
    println(filtered_logrdd.count())
    val comp_errlist = filtered_logrdd.map(l => getComponentErrorList(l)).reduceByKey(_+"::"+_)//.map(m => m._2.split("::").toList)
    comp_errlist.foreach(ce => writer.write(ce._1+" => "+ce._2))
    sc.stop()
    */
  }
}
