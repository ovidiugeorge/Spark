object SparkProgram {


  def function1(x: Int): Int = {
    x
  }

  def function2(x: Int, f: Int => Int): Int = {
    f(x)
  }

  println(function1(100000000))

  def main(args: Array[String]): Unit = {

    //work on jvm
    //variables are mutable
    //one key object of functional programming is to use immutable objects as often possible

    //differente between  var val and def
    // var =>Since it’s mutable, its value may change through the program lifetime.
    // val => It’s an immutable reference, meaning that its value never changes
    // def => def is a function declaration

    // procesare hdfs filse system, s3
    // driver progam  => trimis manager de cluster spark yarn => executa pe hardware corespunzator :) scoate datele le
    // uneste si trece la urmatorul pas


    //Spark =  sparckStreaming  Procesare
    // = machine learning
    // = sql lite
    // = > don't care

    //RDD => resilient distributed dataset (set de date elastice, distribuite)
    // FlatMap vs Map :=> with flatMap i can create other elemnts and with map i cand only transformation

    val hello: String = "Sanatate"
    println(hello)

    var st: String = hello
    st = hello + "there!!!"
    println(st)
    println(f"start : $st ")
    println(s"start : $st ")


    //pattern
    val string: String = "to life 43 sds 67asd"
    val pattern = """.* ([\d]+).*""".r
    val pattern(respond) = string
    val answer = respond.toInt
    print(answer)


    //dealing with booleans
    val a = 1 > 2
    val b = 1 < 2
    val c = a || b
    val d = a && b
    println(s"  $a $b $c $d  ")

    //swith case
    val number = 4

    print(number match {
      case 1 => "UNU"
      case 2 => "DOI"
      case 3 => "TREI"
      case 4 => "PATRU"
      case _ => "something else"
    })

    for (x <- 1 to 2)
      println(x * x)

    //expresision block care return automat o valoarea
    println({
      val v = 10;
    } == ())


    //function
    println(function1(100000000))
    println(function2(10000, x => x + 4 * x))


    //tuplu
    val captainStuff = ("aa", "bbb", "xx")
    println(captainStuff._2)
    println(captainStuff)
    val p = ("p" -> "test", "d" -> "a")
    println(p)


    //List and map
    val list = List("aab", "bbz", "cds", "das ")
    val back = list.map((elem: String) => {
      elem.reverse
    })
    println(back)

    val listNumber = List(1, 2, 3, 4, 5)
    val resultSet = listNumber.reduce((x: Int, y: Int) => {
      x * y + 1
    })

    val resultSetFilter = listNumber.filter((x: Int) => x != 0)
    println(resultSet)

    //Erro and try catch
    // concat list use ++
    //operation : reverse, sorted, ++, max, sum, contains()
    val error = util.Try(-1 / 0) getOrElse "test"
    println(error)

  }


}
