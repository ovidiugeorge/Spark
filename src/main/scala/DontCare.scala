object DontCare {



  %spark

  import spark.implicits._
  import java.util.Calendar
  import scala.collection.mutable.ArrayBuffer
  import java.text.SimpleDateFormat
  case class prodPattern(id_product: Int, start_date: String, end_date: String)

  def generateInterval(arr:Array): Array = {

    //id
    val  id =  arr.head
    for


  }

  def splitDate(idprod:String, start: String, end: String): Array[String] = {

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

    //if date are same don't add at final
    result += sdf.format(final_date.getTime())

    result.toArray
  }


  val firstProd = Array(1, "2018-01-01 15:15:20", "2018-01-03 00:00:00")
  val secondProd = Array(2, "2018-03-01 15:25:20", "2018-02-01 18:00:00")
  val input = Array(firstProd , secondProd)

  val rez = input.map (line => splitDate(line(0).toString,line(1).toString, line(2).toString))

  //val final1 =  rez.map(line =>
  //rez.map(line => println(line.deep.mkString("\n")))
  rez.flatMap(elem => )




}
