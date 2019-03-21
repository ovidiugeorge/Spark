
// is necessary to have a dataframe to realise  union
import org.apache.spark.sql.{Dataset, SparkSession}
case class pattern (userId : Int , start_date: String, end_date: String )
val firstProd = Array(1, "2018-01-01 15:15:20", "2018-01-03 00:00:01")
val secondProd = Array(2, "2018-03-01 15:25:20", "2018-02-01 18:00:00")
val input = Array(firstProd , secondProd)
val  prodFrame = sc.parallelize(input.map(r1 => pattern(r1(0).toString.toInt,r1(1).toString,r1(2).toString))).toDF()

val appended = prodFrame.union(prodFrame)
appended.show()