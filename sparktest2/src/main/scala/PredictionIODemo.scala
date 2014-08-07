/**
 * Created by harsh on 6/8/14.
 */
import io.prediction.Client
import io.prediction.CreateItemRequestBuilder
//import org.apache.spark._

object PredictionIODemo {
val API_KEY = "NO3tJj3XC9JGnszPe9pUMvpq65IJOkMPpb64GeDIKt5iVA0Jwkv2oVS1KznjTUmI"
  val client = new Client(API_KEY)

  def main(args: Array[String]) {
    var ctr = 0
    scala.io.Source.fromFile("/workspaces/harsh/SPARK/sparktest2/src/main/scala/data.txt").getLines().foreach { line =>
      val (uid :: iid :: rate::_) = line.split(",").toList
      //println(s"read line with uid $uid iid $iid rate $rate")

      println(s"line counter : $ctr")
      ctr = ctr +1
      client.identify(uid.toString)

      client.userActionItem(client.getUserActionItemRequestBuilder(uid,"view",iid))
      client.userActionItem(client.getUserActionItemRequestBuilder(uid,"rate",iid).rate(rate.toDouble.toInt))

      client.createUser(uid)
      client.createItem(iid,Array("demoItem"))




      //client.getu
    }
    val userProductActivity = Array(
      (0,5018894,3.5),
      (0,5019089,4.5),
      (0,5018837,5.0),
      (0,5018158,3.0),
      (0,5018594,3.01),
      (0,5019810,4.2)
    )
    userProductActivity.foreach(p =>{
      val uid = p._1.toString
      val iid = p._2.toString
      val rate = p._3
      client.identify(uid.toString)

      client.userActionItem(client.getUserActionItemRequestBuilder(uid,"view",iid))
      client.userActionItem(client.getUserActionItemRequestBuilder(uid,"rate",iid).rate(rate.toDouble.toInt))

      client.createUser(uid)
      client.createItem(iid,Array("demoItem"))
    })
    //client.close()
    Thread.sleep(40000)
    printRecommendation("0")
  }
  def printRecommendation(usrId:String):Unit = {
    //val API_KEY = "NO3tJj3XC9JGnszPe9pUMvpq65IJOkMPpb64GeDIKt5iVA0Jwkv2oVS1KznjTUmI"
    //val client2 = new Client(API_KEY)
    client.identify(usrId.toString)
    val rec =   client.getItemRecTopN("recommendEngineDemo", 5)
    rec.foreach(println)
    client.close()
  }

}

/*
predictionio results with 100 data set :
5016490
5016615
5000040
5014898
5010376

spark results complete data set:
5014675
5014679
5014709
5014717
5014719
---
5001962
5017743
5022682
5010648
5006872
predictionio results with complete data set

5016490
5016615
5000040
5014898
5010376
 */