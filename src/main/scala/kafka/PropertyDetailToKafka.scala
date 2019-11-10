import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.SparkSession

object PropertyDetailToKafka {
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
      .appName("PropertyDetails")
      .getOrCreate()

    val pushdown_query1 = "(SELECT ID, NAME, ADDRESS, ZIP, PRICE_RANGE FROM PROPERTY WHERE PROPERTY.ZIP IN (75252, 75080, 75082, 75074, 75093)) testAlias"
    val collection = spark.read.jdbc(url=jdbcUrl, table=pushdown_query1, properties=connectionProperties)

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
      jaasCfg);

    val kafkaPropsString = new Properties()
    kafkaPropsString.put("bootstrap.servers", "35.226.95.178:9092")
    kafkaPropsString.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer")
    kafkaPropsString.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaPropsString.put("group.id", "test-producer-group")
    kafkaPropsString.put("security.protocol", "SASL_PLAINTEXT")
    kafkaPropsString.put("sasl.mechanism", "PLAIN")
    kafkaPropsString.put("sasl.jaas.config",
      jaasCfg)


    val producerString = new KafkaProducer[Integer, String](kafkaPropsString);
    val producerDouble = new KafkaProducer[Integer, Double](kafkaProps);

    collection.collect().foreach(
      y => {

        //id - int
        val id = y.getInt(0)

        //name - string
        val name = new ProducerRecord[Integer, String]("name", id, y.getString(1))
        producerString.send(name)

        //address - string
        val address = new ProducerRecord[Integer, String]("address", id, y.getString(2))
        producerString.send(address)

        //zip - integer
        val zip = new ProducerRecord[Integer, String]("zip", id, y.getInt(3).toString)
        producerString.send(zip)

        //price-range
        val priceRange = y.getString(4).split('-')
          //min-price - int
          val minPrice = new ProducerRecord[Integer, Double]("minprice", id, priceRange(0).toDouble)
          producerDouble.send(minPrice)

          //max-price - int
          val maxPrice = new ProducerRecord[Integer, Double]("maxprice", id, priceRange(1).toDouble)
          producerDouble.send(maxPrice)

      }
    )
  }
}
