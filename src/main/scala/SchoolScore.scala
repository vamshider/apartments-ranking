import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{min, max, col}
import org.apache.spark.sql.expressions.Window

object SchoolScore {
  def main(args: Array[String]): Unit = {

    val jdbcHostname = "url"
    val jdbcDatabase = "webscrapercs"
    // Create the JDBC URL without passing in the user and password parameters.
    val jdbcUrl = s"jdbc:sqlserver://$jdbcHostname;database=$jdbcDatabase"

    // Create a Properties() object to hold the parameters.
    val connectionProperties = new Properties()

    connectionProperties.put("user", s"username")
    connectionProperties.put("password", s"passsword")

    val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    connectionProperties.setProperty("Driver", driverClass)


    val spark = SparkSession
      .builder()
      .appName("AmenitiesScore")
      .getOrCreate()
    //val sc = spark.sparkContext
    val pushdown_query1 = "(SELECT School.Id, School.Name, Property_school.Property, School.No_of_students, School.Rating FROM School INNER JOIN Property_school ON School.Id = Property_school.School WHERE Property_school.Property IN (SELECT ID FROM PROPERTY WHERE PROPERTY.ZIP IN (75252, 75080, 75082, 75074, 75093))) testAlias"
    val dataSchools = spark.read.jdbc(url=jdbcUrl, table=pushdown_query1, properties=connectionProperties).toDF("schoolid", "schoolname", "propertyid", "numberstudents", "rating")
    val avgStudentsRating = dataSchools.groupBy("propertyid").avg("numberstudents", "rating").orderBy("propertyid").toDF("propertyid", "avgNumberStudents", "avgRating")
    val minAvgStudents = avgStudentsRating.agg(min("avgNumberStudents"))
    val minAvgSchoolRating = avgStudentsRating.agg(min("avgRating"))

    val minAvgStudentsValue = minAvgStudents.head().getDouble(0)
    val minAvgSchoolRatingValue = minAvgSchoolRating.head().getDouble(0)

    val schoolsWeightsRDD = avgStudentsRating.rdd.map(x => (x.getInt(0), x.getDouble(1)/minAvgStudentsValue + x.getDouble(2)/minAvgSchoolRatingValue))


    //Max Students School and School Name
    val windowSpecRating = Window.partitionBy("propertyid").orderBy("propertyid")
    val maxSchoolRatingData = dataSchools.withColumn("maxRating", max("rating") over windowSpecRating).filter(col("maxRating") === col("rating")).drop("rating", "numberstudents").toDF("schoolIdWithMaxRating", "schoolname","propertyid","maxRating")
    val maxRatingSchoolRDD = maxSchoolRatingData.rdd.map(x => (x.getInt(2), (x.getInt(0), x.getString(1), x.getInt(3))))

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

    val kafkaPropsString = new Properties()
    kafkaPropsString.put("bootstrap.servers", "35.226.95.178:9092")
    kafkaPropsString.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer")
    kafkaPropsString.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaPropsString.put("group.id", "test-producer-group")
    kafkaPropsString.put("security.protocol", "SASL_PLAINTEXT")
    kafkaPropsString.put("sasl.mechanism", "PLAIN")
    kafkaPropsString.put("sasl.jaas.config",
      jaasCfg)
    // school score
    schoolsWeightsRDD.collect.foreach(
      y => {
        val producer = new KafkaProducer[Integer, Double](kafkaProps)
        val z = new ProducerRecord[Integer, Double]("schoolscore",y._1,y._2)
        producer.send(z)
        producer.close()
    })

    //school name
    maxRatingSchoolRDD.collect.foreach(x => {
      val producer = new KafkaProducer[Integer, String](kafkaPropsString)
      val z = new ProducerRecord[Integer, String]("schoolname",x._1,x._2._2)
      producer.send(z)
      producer.close()
    })

    //max school rating
    maxRatingSchoolRDD.foreachPartition(x => {
      val producer = new KafkaProducer[Integer, Double](kafkaProps)
      x.foreach( x => {
        val z = new ProducerRecord[Integer, Double]("schoolrating",x._1,x._2._3.toDouble)
        producer.send(z)
      })
      producer.close()
    })

  }
}
