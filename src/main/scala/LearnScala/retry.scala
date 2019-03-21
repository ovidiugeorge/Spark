package LearnScala

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import scala.collection.mutable.ArrayBuffer

object retry {
  Logger.getLogger("org").setLevel(Level.ERROR)
  case class pattern (userId : Int , start_date: String, end_date: String )

  def splitDate(start: String, end: String): Array[String] = {

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val start_date = sdf.parse(start)
    val final_date = sdf.parse(end)

    val c = Calendar.getInstance()
    c.setTime(start_date)
    val result = ArrayBuffer("")
    c.add(Calendar.DATE, 1)
    c.set(Calendar.SECOND, 0)
    c.set(Calendar.HOUR, 12)
    c.set(Calendar.MINUTE, 0)
    val dt = sdf.format(c.getTime)
    result+= dt
    result.toArray
    //do {
    //
    //}
    //while()
//    Array("2018-03-01 15:25:20", "2018-02-014 00:00:00")
  }

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local[*]", "retry")
    val sqlContext = new SQLContext(sc)

    val firstProd = Array(1, "2018-01-01 15:15:20", "2018-01-03 00:00:00")
    val secondProd = Array(2, "2018-03-01 15:25:20", "2018-02-014 00:00:00")
    val input = Array(firstProd , secondProd)

    //val  prodFrame = sc.parallelize(input.map(r1 => pattern(r1(0).asInstanceOf[Int],r1(1).toString,r1(2).toString))).toDF()
    //prodFrame.show()

    Array(splitDate("2018-03-01 15:25:20", "2018-03-03 00:00:00")).map(line => println(line.deep.mkString("\n")))

  }


}
