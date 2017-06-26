package weblog

import java.nio.file.Paths
import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.streaming.{Duration, Minutes}

import scala.collection.mutable.ListBuffer

/**
  * 2015-07-22T09:00:28.048369Z marketpalce-shop 180.179.213.94:48725 10.0.6.108:80 0.00002 0.002333 0.000021 200 200 0 35734 "GET https://paytm.com:443/shop/p/micromax-yu-yureka-moonstone-grey-MOBMICROMAX-YU-DUMM141CD60AF7C_34315 HTTP/1.0" "-" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2
  * @param timestamp: The time when the load balancer received the request from the client, in ISO 8601 format.
  * @param elb: The name of the load balancer
  * @param clientIp: The IP address and port of the requesting client.
  * @param serverIp: The IP address and port of the registered instance that processed this request.
  * @param requestProcessingTime
  * @param backendProcessingTime
  * @param responseProcessingTime
  * @param elbStatusCode: [HTTP listener] The status code of the response from the load balancer
  * @param backendStatusCode
  * @param receivedBytes
  * @param sentBytes
  * @param request
  * @param userAgent
  * @param sslCipher
  * @param sslProtocol
  */
case class WebLogExtended (
                    timestamp: Long,
                    elb: String,
                    clientIp: String,
                    serverIp: String,
                    requestProcessingTime: Double,
                    backendProcessingTime: Double,
                    responseProcessingTime: Double,
                    elbStatusCode: Int,
                    backendStatusCode: Int,
                    receivedBytes: Double,
                    sentBytes: Double,
                    request: String,
                    userAgent: String,
                    sslCipher: String,
                    sslProtocol: String,
                    session: Int = 0,                //calculated field
                    intervalBetween2Req: Long = 0l   //calculated field
                  )

case class WebLog (
                    timestamp: Long,
                    clientIp: String,
                    request: String,
                    userAgent: String,
                    session: Int = 0,
                    intervalBetween2Req: Long = 0l
                  )
object WebLogMainApp extends App {

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("WebLogChallenge")
      .config("spark.master", "local[*]")
      .getOrCreate()

  val rawData: RDD[String] = spark.sparkContext
    //.textFile(fsPath("/2015_07_22_mktplace_shop_web_log_sample.log"))
    .textFile(fsPath("/sample.log"))

  //1158500
  println(s"count rows : ${rawData.count()}")

  val dataDF: RDD[WebLog] = rawData.flatMap(converter(_)) // flatMap will drop None options.

  dataDF take 2 foreach println

  val dataDFSessionned = dataDF.groupBy(_.clientIp).flatMapValues(l => getSessions(l.toSeq, Minutes(15)))

  val dataRDD = dataDFSessionned.map { case (key, line) => line}

  val sqlContext = new SQLContext(spark.sparkContext)
  import sqlContext.implicits._

  val dataDataframe = dataRDD.toDF()

  // Determine the average session time
  val dfAverageSessionTime = averageSessionTime(dataDataframe)

  //  Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.
  val uniqueURLPerSession: DataFrame = uniqueURLPerSession(dataDataframe)

  val mostEngagedUser: String = mostEngagedUser(dataDataframe)

  import org.apache.spark.sql.functions._

  // Determine the average session time
  def averageSessionTime(df: DataFrame): DataFrame = {
    df
      .groupBy("clientIp", "session")
      .agg(round(avg("intervalBetween2Req"), 1))
      .orderBy("clientIp", "session")
  }

   // Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.
  def uniqueURLPerSession(df: DataFrame): DataFrame = {
    df
      .groupBy("clientIp", "session", "request")
      .count()
      .filter($"count" === 1)
  }

   // Find the most engaged users, ie the IPs with the longest session times
   def mostEngagedUser(df: DataFrame): String = {
     df
       .groupBy("clientIp", "session")
       .agg(sum("intervalBetween2Req"))
       .orderBy($"sum(intervalBetween2Req)".desc)
       .first()(0).toString
   }

  /**
    * Convert an entry line into a WebLog
    * @param line
    * @return
    */
   def converter(line: String): Option[WebLog] = {
    try {
      //ignoring space under quotes
      val fields = line.split(" (?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1).map(_.trim).toList
      Some(
        WebLog (
          timestamp = dateFormat.parse(fields(0)).getTime,
          clientIp = fields(2),
          request = fields(11),
          userAgent = fields(12)
        )
      )
    } catch {
      case e: Exception => None
    }
  }

  /**
    * Define all the sessions per clientIp
    * @param lines
    * @param timeout
    * @return
    */
  def getSessions (lines: Seq[WebLog], timeout: Duration): Seq[WebLog] = {
    if (lines.size < 2) {
        lines
    } else {
        val sorted = lines.sortBy(_.timestamp)
        val tupleList = (sorted drop 1, sorted).zipped
        var newList = new ListBuffer[WebLog]()
        newList += sorted.head
        tupleList.map {
          case (l,r) =>
            val diffTime = l.timestamp - r.timestamp
            val (calcSes, interval) = if (diffTime > timeout.milliseconds) {
              (newList.last.session + 1, 0l)
            } else {
              (newList.last.session, diffTime)
            }
            newList += l.copy(session = calcSes, intervalBetween2Req = interval)
            diffTime
        }
        newList
    }
  }

  def fsPath(resource: String): String =
    Paths.get(getClass.getResource(resource).toURI).toString

}
