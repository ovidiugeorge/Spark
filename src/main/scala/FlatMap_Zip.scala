object FlatMap_Zip {


  def generateInterval(arr: Array[Any]): Array[Any] = {

    //id
    val id = arr.head

    val right = arr.tail.tail
    val left = arr.tail.dropRight(1)
    val res = (left zip right)
    res.map(line => (id, line._1,line._2))
  }

  def main(args: Array[String]): Unit = {


    val firstProd = Array(Array(1, "1", "2", "3", "4", "5"), Array(3, "11", "21", "31", "41", "51"))
    val res = firstProd.flatMap(line => generateInterval(line))
    val t = (1,"43",5).productIterator.toArray

  }

}
