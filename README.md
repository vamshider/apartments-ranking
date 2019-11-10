# apartments-ranking
This project ranks the Apartments in multiple U.S. regions using Big Data Technologies and Machine Learning implementations.

The classes used are :
   *	AmenitiesScore  
   *	ExpensesScore
   *	ReviewScore
   *	SchoolScore
   *	PropertyDetailToKafka

The parameters to connect to the database are to be replaced .

Setup a kafka cluster and make sure you overwrite ip address and port of the broker in the source code and rebuild the jar.

Setup elastic search and configure the authentication parameters in logstash, which is installed on kafka. 

To run the application on AWS:
First create a spark cluster on AWS and run the following commands:
  *	spark-submit --deploy-mode cluster --class AmenitiesScore <path to jar>
  *	spark-submit --deploy-mode cluster --class ExpensesScore <path to jar>
  *	spark-submit --deploy-mode cluster --class ReviewScore <path to jar>
  *	spark-submit --deploy-mode cluster --class SchoolScore <path to jar>

  * spark-submit --deploy-mode cluster --class PropertyDetailToKafka  <path to jar>

Data can now be displayed on kibana, and filters could be added on top of it.
