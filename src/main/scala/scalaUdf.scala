import java.text.SimpleDateFormat
import java.util.Calendar
val calcDatesBetween = udf((startDate: String, endDate: String) => {
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val startDateFormated = dateFormat.parse(startDate)
  val endDateFormated = dateFormat.parse(endDate)

  val calStart = Calendar.getInstance()
  calStart.setTime(startDateFormated)

  val calEnd = Calendar.getInstance
  calEnd.setTime(endDateFormated)

  var finalString = ""

  if (!(calStart.compareTo(calEnd) == -1)) {
    finalString += dateFormat.format(calStart.getTime) + "|" + dateFormat.format(calEnd.getTime)
  } else {
    while (calStart.before(calEnd)) {
      finalString += dateFormat.format(calStart.getTime) + "|"
      calStart.add(Calendar.DATE, 1)
      calStart.set(Calendar.HOUR_OF_DAY, 0)
      calStart.set(Calendar.MINUTE, 0)
      calStart.set(Calendar.SECOND, 0)
      calStart.set(Calendar.MILLISECOND, 0)

      if (calStart.compareTo(calEnd) == -1) {
        finalString += dateFormat.format(calStart.getTime) + ","
      } else {
        finalString += dateFormat.format(calEnd.getTime)
      }
    }
  }
  finalString
})

//Apply UDF

spark.read.parquet("/testData/Alexandru/PriceIntervalsRo").where($"mkt_product_id" === 1774 && $"StartDate" === "2016-11-22 00:49:16")
  .withColumn("intervals", explode(split(calcDatesBetween($"StartDate", $"EndDate"), ",")))
  .withColumn("int_start", split($"intervals", "[|]")(0))
  .withColumn("int_end", split($"intervals", "[|]")(1))
  .drop($"intervals")
  .orderBy($"int_start")
  .show(false)

