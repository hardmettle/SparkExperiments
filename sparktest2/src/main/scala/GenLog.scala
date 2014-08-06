/**
 * Created by harsh on 1/8/14.
 */
import java.io.{File, PrintWriter}
import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date

import org.slf4j.{LoggerFactory, Logger}

/**
 * Created by harsh on 30/7/14.
 */
case class Logging(time: Date, logType: String, module: String, message: String, detail: Option[Any], time_in_ms: Long) {
  val d = detail match {
    case Some(dtl) => dtl.toString
    case None => ""
  }
  override def toString: String = time + " : [" + logType +"]\t:\t"+ module + "\t:\t " + message + "," + d + " " + time_in_ms
}

object GenLog {
  def main(args: Array[String]) {
    val type_list = List("INFO","ERROR","WARN","DEBUG")
    val module_list = List("UI","API","REPORT","ORDER")
    var ctr = 1
    val writer = new PrintWriter((new File("/workspaces/harsh/log_example.txt")))
    //val log: Logger = LoggerFactory.getLogger(Logging.getClass)
    val i :Long =0
    for ( i <- 1L to 100000L) {

      val logType = type_list(ctr-1)

      val module = module_list(ctr-1)

      if(ctr > 3){
        ctr = 1
      }else{
        ctr = ctr + 1
      }

      val time: Date = new Date
      val message = if (i % 2 == 0) {
        "HI !! ITS AN EVEN TEST MESSAGE"
      } else {
        "HELLO !! ITS AN ODD TEST MESSAGE"
      }
      val detail = if (i % 2 == 0) {
        Some("Hi its and even test detail")
      } else {
        None
      }
      val time_in_ms = i

      val l: Logging = new Logging(time,logType,module,message,detail,time_in_ms)

      writer.write(l.toString)
      writer.println()


    }
    writer.flush()
    writer.close()

  }

}