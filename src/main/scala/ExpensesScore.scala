import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.max


object ExpensesScore {
  def main(args: Array[String]): Unit = {

    val jdbcHostname = "url"
    val jdbcDatabase = "webscrapercs"
    // Create the JDBC URL without passing in the user and password parameters.
    val jdbcUrl = s"jdbc:sqlserver://$jdbcHostname;database=$jdbcDatabase"

    // Create a Properties() object to hold the parameters.
    val connectionProperties = new Properties()

    connectionProperties.put("user", s"username")
    connectionProperties.put("password", s"password")

    val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    connectionProperties.setProperty("Driver", driverClass)


    val spark = SparkSession
      .builder()
      .appName("AmenitiesScore")
      .getOrCreate()
    import spark.implicits._
    //val sc = spark.sparkContext
    val pushdown_query1 = "(SELECT Cost, Property FROM Expenses WHERE Expenses.Property IN (SELECT ID FROM PROPERTY WHERE PROPERTY.ZIP IN (75252, 75080, 75082, 75074, 75093) AND SUBSTRING(PROPERTY.PRICE_RANGE, 0, 4) != '0-0'))) testAlias"
    val dataExpenses = spark.read.jdbc(url=jdbcUrl, table=pushdown_query1, properties=connectionProperties)

    val pushdown_query2 = "(SELECT ID, PRICE_RANGE FROM PROPERTY WHERE PROPERTY.ZIP IN (75252, 75080, 75082, 75074, 75093) AND SUBSTRING(PROPERTY.PRICE_RANGE, 0, 4) != '0-0') test3"
    val dataRent = spark.read.jdbc(url=jdbcUrl, table=pushdown_query2, properties=connectionProperties)

    val mappedExpenses = dataExpenses.map(x => (x.getString(0).split("-"), x.getInt(1)))
    val mappedRent = dataRent.map(x => (x.getInt(0), x.getString(1).split("-")))

    val expenseData = mappedExpenses.map{case (Array(a:String, b:String), c:Int) => (a.toInt,b.toInt,c.toInt)}.toDF("min", "max", "propertyid")
    val rentData = mappedRent.map{case (c:Int, Array(a:String, b:String)) => (a.toInt,b.toInt,c.toInt)}.toDF("min", "max", "propertyid")
    val data = expenseData.union(rentData)
    val sumExpensesRent = data.groupBy("propertyid").sum("min", "max").orderBy("propertyid").toDF("propertyid", "minExpenseRent", "maxExpenseRent")
    val maxMinExpensesRent = sumExpensesRent.agg(max("minExpenseRent"))
    val maxMaxExpensesRent = sumExpensesRent.agg(max("maxExpenseRent"))

    val maxMinValue = maxMinExpensesRent.head().getLong(0).toDouble
    val maxMaxValue = maxMaxExpensesRent.head().getLong(0).toDouble

    val expensesWeightsRDD = sumExpensesRent.rdd.map(x => (x.getInt(0), maxMinValue/x.getLong(1).toDouble + maxMaxValue/x.getLong(2).toDouble))




    val jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";"
    val jaasCfg = String.format(jaasTemplate, "user", "YpaoVXFeDFw7")

    val kafkaProps = new Properties()
    kafkaProps.put("bootstrap.servers", "35.226.95.178:9092")
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer")
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.DoubleSerializer")
    kafkaProps.put("group.id", "test-producer-group")
    kafkaProps.put("security.protocol", "SASL_PLAINTEXT")
    kafkaProps.put("sasl.mechanism", "PLAIN")
    kafkaProps.put("sasl.jaas.config",
      jaasCfg)

    expensesWeightsRDD.foreachPartition(x => {
      val producer = new KafkaProducer[Integer, Double](kafkaProps)
      x.foreach(y => {
        val z = new ProducerRecord[Integer, Double]("expense",y._1,y._2)
        producer.send(z)
      })
      producer.close()
    })

  }
}
