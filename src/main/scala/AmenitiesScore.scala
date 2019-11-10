import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql._
import java.util.Properties

object AmenitiesScore {
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
      val pushdown_query1 = "(SELECT AMENITY, Property FROM PROPERTY_AMENITY_MAP WHERE PROPERTY_AMENITY_MAP.Property IN (SELECT ID FROM PROPERTY WHERE PROPERTY.ZIP IN (75252, 75080, 75082, 75074, 75093))) testAlias"
    val collection = spark.read.jdbc(url=jdbcUrl, table=pushdown_query1, properties=connectionProperties)
      val count = collection.count()
      val sn = collection.map(x => (x.getInt(0),x.getInt(1)))
    val sn1 = sn.map{case (x:Int,y:Int) => ((y,x),1)}
    val tF = sn1.rdd.reduceByKey((x, y) => x + y)


    val idf1 = sn1.rdd.reduceByKey((x, y) => x).map( x => (x._1._2, x._2)).reduceByKey((x, y) => x + y).map{case (x:Int, y:Int)=> (x, math.log(count/y))}
    val weights = tF.map(x => (x._1._2, x)).join(idf1).map(x => (x._2._1._1._1, (x._1, x._2._1._2 * x._2._2)))
    val AmScore = weights.map(x => (x._1.toInt,x._2._2.toDouble)).reduceByKey((x,y) => x+y)


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

    AmScore.foreachPartition(x => {
      val producer = new KafkaProducer[Integer, Double](kafkaProps)
      x.foreach(y => {
        val z = new ProducerRecord[Integer, Double]("amenity",y._1,y._2)
        producer.send(z)
      })
      producer.close()
    })
  }
}
