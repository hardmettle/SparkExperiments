/**
 * Created by harsh on 4/8/14.
 */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

//case class Product(id:Long,category:String,quantity:Int,price:Double,details:String)
case class Params(input: String = "/workspaces/harsh/SPARK/sparktest2/src/main/scala/data.txt",
                  kryo: Boolean = false,
                  numIterations: Int = 5,
                  lambda: Double = 1.0,
                  rank: Int = 10,
                  implicitPrefs: Boolean = false)
object ProductPrediction {
  val params = new Params()
  val userProductActivity = Array(
                                    Rating(0,5018894,3.5),
                                    Rating(0,5019089,4.5),
                                    Rating(0,5018837,5.0),
                                    Rating(0,5018158,3.0),
                                    Rating(0,5018594,3.01),
                                    Rating(0,5019810,4.2)
                            )

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName(s"Product Prediction").setMaster("local")
    val sc = new SparkContext(conf)
    val userProdActivityRdd = sc.parallelize(userProductActivity)
    val productRange = sc.textFile(params.input).map { line =>
      val fields = line.split(",")
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
    }.cache()
    val numRatings = productRange.count()
    val numUsers = productRange.map(_.user).distinct().count()
    val numProds = productRange.map(_.product).distinct().count()

    println(s"Got $numRatings ratings from $numUsers users on $numProds products.")

    val splits = productRange.randomSplit(Array(0.8, 0.2))
    val training = splits(0).cache().union(userProdActivityRdd)
    val model = ALS.train(training, params.rank, params.numIterations,params.lambda)
    val userChoiceProd = userProdActivityRdd.map(_.product).collect().toSet
    val tmp = productRange.filter(p=> !userChoiceProd.contains(p.product)).map(p2 => (p2.product,0) )
    val remainingProd = tmp.groupByKey().map(t=>(0,t._1))

    val recommendations = model.predict(remainingProd).collect().sortBy(-_.rating).take(5)
    println("Recommended product Ids for you")
    recommendations.foreach(r => println(r.product))
  }
}
