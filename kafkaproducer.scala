package kafkaspark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import requests._
object kafkaproducer {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("My Spark Application")
      .master("local[*]")
      .getOrCreate()
    while (true) {
      import spark.implicits._
      val apiUrl = "https://api.tfl.gov.uk/Line/victoria/Arrivals?app_id=92293faa428041caad3dd647d39753a0&app_key=ba72936a3db54b4ba5792dc8f7acc043"
      val response = get(apiUrl, headers = headers)
      val total = response.text()
      val dfFromText = spark.read.json(Seq(total).toDS)

      // select the columns you want to include in the message

      val messageDF = dfFromText.select($"id", $"stationName", $"lineName", $"towards",
        $"expectedArrival",$"vehicleId",$"platformName",$"direction",$"destinationName",
        $"timestamp",$"timeToStation", $"currentLocation",$"timeToLive")

      val kafkaServer: String = "ip-172-31-8-235.eu-west-2.compute.internal:9092,ip-172-31-14-3.eu-west-2.compute.internal:9092"
      val topicSampleName: String = "arrivaldata1"

      messageDF.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value").selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").write.format("kafka").option("kafka.bootstrap.servers", kafkaServer).option("topic", topicSampleName).save()
      println("message is loaded to kafka topic")
      Thread.sleep(10000) // wait for 10 seconds before making the next call
    }
  }
}