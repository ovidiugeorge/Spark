import java.util.Calendar
import scala.collection.mutable.ArrayBuffer
import java.text.SimpleDateFormat

case class pattern (userId : Int , start_date: String, end_date: String )

def generateInterval(arr: Array[Any]): Array[Array[Any]] = {
  val id = arr.head
  val right = arr.tail.tail
  val left = arr.tail.dropRight(1)
  val res = (left zip right)
  res.map(line => Array(id, line._1,line._2))

}

def splitDate(idprod:String, start: String, end: String): Array[Any] = {

  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val result = ArrayBuffer(idprod,start)

  val start_date = Calendar.getInstance()
  val final_date = Calendar.getInstance()

  final_date.setTime(sdf.parse(end))
  start_date.setTime(sdf.parse(start))

  start_date.add(Calendar.DATE, 1)
  start_date.set(Calendar.SECOND, 0)
  start_date.set(Calendar.HOUR_OF_DAY, 0)
  start_date.set(Calendar.MINUTE, 0)

  while(start_date.get(Calendar.DAY_OF_MONTH)<final_date.get(Calendar.DAY_OF_MONTH)) {
    result += sdf.format(start_date.getTime())
    start_date.add(Calendar.DAY_OF_MONTH, 1)
  }

  if(start_date.get(Calendar.DAY_OF_MONTH)==final_date.get(Calendar.DAY_OF_MONTH))
    result += sdf.format(start_date.getTime())

  //not add same date in list
  if(start_date.compareTo(final_date)!=0)
    result += sdf.format(final_date.getTime())

  result.toArray
}


val firstProd = Array(1, "2018-01-01 15:15:20", "2018-01-03 00:00:01")
val secondProd = Array(2, "2018-03-01 15:25:20", "2018-02-01 18:00:00")
val input = Array(firstProd , secondProd)

val splitIntoIntervales = input.map (line => splitDate(line(0).toString,line(1).toString, line(2).toString))
val generateIntervales = splitIntoIntervales.flatMap(line => generateInterval(line))
//val test = generateIntervales.map(line => line.productIterator.toList)

val  prodFrame = sc.parallelize(generateIntervales.map(r1 => pattern(r1(0).toString.toInt,r1(1).toString,r1(2).toString))).toDF()
prodFrame.show()




