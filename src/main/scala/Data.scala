
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j._
import org.apache.spark._

import scala.collection.mutable.ArrayBuffer

case class Department(id_product: Int, start_date: String, end_date: String)

object Data {

  def splitDate(start: String, end: String): Array[String] = {

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val result = ArrayBuffer(start)

    val start_date = Calendar.getInstance()
    val final_date = Calendar.getInstance()

    final_date.setTime(sdf.parse(end))
    start_date.setTime(sdf.parse(start))

    start_date.add(Calendar.DATE, 1)
    start_date.set(Calendar.SECOND, 0)
    start_date.set(Calendar.HOUR_OF_DAY, 0)
    start_date.set(Calendar.MINUTE, 0)



    while(start_date.get(Calendar.DAY_OF_MONTH)<final_date.get(Calendar.DAY_OF_MONTH))
    {

      result += sdf.format(start_date.getTime());
      start_date.add(Calendar.DAY_OF_MONTH, 1)

    }

    result += sdf.format(start_date.getTime());
    result += sdf.format(final_date.getTime())

    //result
    result.toArray
  }

  def main(args: Array[String]): Unit = {
    val firstProd = Array(1, "2018-01-01 15:15:20", "2018-01-03 00:00:00")
    val secondProd = Array(2, "2018-03-01 15:25:20", "2018-02-014 00:00:00")
    val input = Array(firstProd , secondProd)


    val rez = input.map (line => Array(line ++  Array("2018-03-01 15:25:20", "2018-02-014 00:00:00") ) )
    rez.map (line => println(line.deep.mkString("\n")))
    //Array(splitDate("2018-03-01 15:25:20", "2018-03-03 00:00:00")).map(line => println(line.deep.mkString("\n")))
    //val sc = new SparkContext("local[*]","Data")
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
   // val dataFrame = input.toDF




  }


}
