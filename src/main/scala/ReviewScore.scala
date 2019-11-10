import java.util.Properties
import com.databricks.spark.corenlp.functions._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object ReviewScore {
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
    val sc = spark.sparkContext
    val pushdown_query1 = "(SELECT Property, Content, Rating FROM review WHERE review.Property IN (SELECT ID FROM PROPERTY WHERE PROPERTY.ZIP IN (75252, 75080, 75082, 75074, 75093))) testAlias"
    val dataReviews = spark.read.jdbc(url=jdbcUrl, table=pushdown_query1, properties=connectionProperties).toDF("id", "text", "rating")
    import spark.sqlContext.implicits._
    val version = "3.7.0"
    val baseUrl = s"http://repo1.maven.org/maven2/edu/stanford/nlp/stanford-corenlp"
    val model = s"stanford-corenlp-$version-models.jar"
    val url = s"$baseUrl/$version/$model"
    if (! sc.listJars().exists(jar => jar.contains(model))) {
      import scala.sys.process._
      s"wget -N $url".!!
      s"jar xf $model".!!
      sc.addJar(model)
    }
    val output = dataReviews
      .select('id, explode(ssplit('text)).as('text), 'rating)
      .select('id, 'text, sentiment('text).as('sentiment), 'rating)
    val avgReviewsRating = output.groupBy("id").avg("sentiment", "rating").orderBy("id").toDF("propertyid", "avgSentiment", "avgRating")
    val reviewsWeightsRDD = avgReviewsRating.rdd.map(x => (x.getInt(0), x.getDouble(1) + x.getDouble(2)))
    val jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";"
    val jaasCfg = String.format(jaasTemplate, "user", "YpaoVXFeDFw7")
    val kafkaProps = new Properties()
    kafkaProps.put("bootstrap.servers", "35.188.143.51:9092")
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer")
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.DoubleSerializer")
    kafkaProps.put("group.id", "test-producer-group")
    kafkaProps.put("security.protocol", "SASL_PLAINTEXT")
    kafkaProps.put("sasl.mechanism", "PLAIN")
    kafkaProps.put("sasl.jaas.config",
      jaasCfg)
    val producer = new KafkaProducer[Integer, Double](kafkaProps)
    reviewsWeightsRDD.collect.foreach(y => {
      val z = new ProducerRecord[Integer, Double]("reviewscore",y._1,y._2)
      producer.send(z)
    })
    producer.close()
  }
}