

/***
Cerinta: Sa se comenzile clientilor din anul 2019 => client - zi
  1.Valoarea totala comenzilor
  2.Numarul comenzilor
  3.Total discount
  4.Numar total comenzi(overall)
  5.Valoare totala(overall)
***/


/**  Fine version !! **/
import org.apache.spark.sql.expressions.Window
val window =  Window.partitionBy("customers_id_assoc")

//Cum putem rezolva aceasta problema cu join.
val ord = orders.where( $"platform_id" === 1 && $"data" >= "2019-01-01")
  .groupBy("customers_id_assoc", "data")
  .agg(round(sum($"products_price" * $"cantitate"),4).alias("total_price"),
        countDistinct("id_comanda").alias("nr_of_orders"),
        sum("total_discount_value").alias("total_discount"))
  .withColumn("total_order_year", sum($"nr_of_orders") over window)
  .withColumn("Value_total", sum($"total_price") over window)

z.show(ord)



/**  Other version for this used JOIN **/






//======================================================================================================================