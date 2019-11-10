package com.hari.learning.spark.realtime

import org.apache.spark.sql.{ SparkSession, Dataset, Row }
import org.apache.spark.sql.types.{ StructType, StructField, IntegerType, StringType }
import org.apache.spark.sql.functions.{ from_json, to_json }
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener._
import org.apache.spark.sql.streaming.StreamingQueryManager
import org.apache.spark.sql.catalyst.expressions._
import com.hari.learning.spark.realtime.IngestTicketIntoFF.CustomStreamingLister

/**
 * Reads data from Tickets topic and stages it to a flat-file (HDFS/S3/ local File)
 * Let us assume the structure of the json payload is
 * {"tickId":"Ticket1","category":"Severe","desc":"Description of Ticket Id's containing some relevant key words."}
 * where tickId represents the id of the ticket, category - a finite set of values , desc represents the description of the
 * ticket raised which would be later used to classify into appropriate categories.
 *
 * @author harikrishna
 *
 */

class IngestTicketIntoFF(streamingList: CustomStreamingLister) {

  def ingestFromKafkaToFF(broker: String, topic: String, targetFF: String, queryName: String): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("StreamDataFromKafka").getOrCreate
    import spark.implicits._
    val kafka = spark.readStream.format("kafka").option("kafka.bootstrap.servers", broker)
      .option("subscribePattern", topic).option("startingOffsets", "earliest").load
    // representing json as a struct type.
    val destSchema = StructType(new StructField("tickId", StringType, true)
      :: new StructField("category", StringType, true)
      :: new StructField("desc", StringType, true)
      :: Nil)
    kafka.select(from_json('value.cast(StringType), destSchema)).toDF("Ticket").writeStream
      .queryName(queryName).outputMode("append").trigger(ProcessingTime("10 seconds")).foreachBatch {
        (inp: Dataset[Row], _: Long) =>
          inp.printSchema
          inp.select("Ticket.tickId", "Ticket.category", "Ticket.desc").write.json(targetFF)
      }.start()
    streamingList.qm = spark.streams
    spark.streams.addListener(streamingList)
    spark.streams.awaitAnyTermination
  }

}

object IngestTicketIntoFF {

  trait CustomStreamingLister extends StreamingQueryListener {
    var qm: StreamingQueryManager = null // this needs to be set by all implementations
  }

  val defaultList = new CustomStreamingLister {
    override def onQueryStarted(qs: QueryStartedEvent): Unit = {}
    override def onQueryProgress(qp: QueryProgressEvent): Unit = {}
    override def onQueryTerminated(qt: QueryTerminatedEvent): Unit = {}
  }

  def startIngestion(broker: String, topic: String, targetFile: String, query: String)(streamList: CustomStreamingLister = defaultList): Unit = new IngestTicketIntoFF(streamList).ingestFromKafkaToFF(broker, topic, targetFile, query)
}