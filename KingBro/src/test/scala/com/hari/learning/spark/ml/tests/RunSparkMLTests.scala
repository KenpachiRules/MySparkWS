package com.hari.learning.spark.ml.tests

import org.apache.kafka.clients.admin.{ AdminClient, NewTopic }
import org.testng.annotations.{ BeforeSuite, Test, AfterSuite, Parameters }
import java.util.Properties
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import org.apache.kafka.clients.admin.TopicListing
import com.hari.learning.spark.batch.Classification
import com.hari.learning.spark.realtime.IngestTicketIntoFF
import com.hari.learning.spark.realtime.IngestTicketIntoFF.CustomStreamingLister
import com.hari.learning.kafka.MyKafkaProducer
import java.util.concurrent.TimeUnit
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener._

object RunSparkMLTests {

  val sampleTickets = List("{\"tickId\":\"Ticket1\",\"category\":\"p0\",\"desc\":\" Blocks from further testing , The ticket is critical \"}", "{\"tickId\":\"Ticket2\",\"category\":\"p2\",\"desc\":\" Display not working \"}", "{\"tickId\":\"Ticket3\",\"category\":\"p3\",\"desc\":\" Letters need to be in bold \"}", "{\"tickId\":\"Ticket4\",\"category\":\"p2\",\"desc\":\" Display not working \"}", "{\"tickId\":\"Ticket5\",\"category\":\"p0\",\"desc\":\" Installer not working , The ticket is critical \"}", "{\"tickId\":\"Ticket6\",\"category\":\"p0\",\"desc\":\" Crashing with OOM , The ticket is critical \"}", "{\"tickId\":\"Ticket7\",\"category\":\"p0\",\"desc\":\" Application timing out , The ticket is critical \"}", "{\"tickId\":\"Ticket8\",\"category\":\"p0\",\"desc\":\" Heartbeat failed to establish connecion , The ticket is critical \"}")

  @Test
  @Parameters(Array("broker", "topic", "partitions", "replication_factor"))
  def createKafkaTopic(broker: String, topic: String, partitions: String, replication_factor: String): Unit = {
    val kfkProps = new Properties
    kfkProps.put("bootstrap.servers", broker)
    val topic1 = new NewTopic(topic, partitions.toInt, replication_factor.toShort)
    val adminClient = AdminClient.create(kfkProps)
    val result = adminClient.createTopics(List(topic1))
    // wait synchronously until the topic is created.
    result.all.get
    var finalResult: TopicListing = null
    adminClient.listTopics().listings().get().iterator().foreach(top => if (topic.equals(top.name())) finalResult = top)
    assert(finalResult != null, s" Topic ${topic} could not be created.")
    // produce data to the created kafka topic
    val myKfkProd = MyKafkaProducer.newInstance[String]().get
    val prod = myKfkProd.getProducer.apply(broker, "org.apache.kafka.common.serialization.StringSerializer")
    sampleTickets.foreach(ticket => prod.send(myKfkProd.getProducerRec.apply(topic, ticket)))
    prod.close(10, TimeUnit.SECONDS)
    println(s" ------------> Topic ${topic} created successfully <--------------")
  }

  @Test(dependsOnMethods = Array("createKafkaTopic"))
  @Parameters(Array("broker", "topic", "targetFile", "queryName"))
  def streamDataFromKafkaToFF(broker: String, topic: String, targetFile: String, queryName: String): Unit = {
    var recCount: Long = 0
    val listener = new CustomStreamingLister {
      override def onQueryStarted(qs: QueryStartedEvent): Unit = {}
      override def onQueryProgress(qp: QueryProgressEvent): Unit = {
        recCount += qp.progress.numInputRows
        if (recCount >= 8) {
          // terminate the query.
          val qName = qm.active.filter(q => queryName.equals(q.name)).foreach(q => q.stop)
        }
      }
      override def onQueryTerminated(qt: QueryTerminatedEvent): Unit = {}
    }
    IngestTicketIntoFF.startIngestion(broker, topic, targetFile, queryName)(listener)
    println(" ---------------> Data streamed from kafka to FF <-------------- ")
  }

  @Test(dependsOnMethods = Array("streamDataFromKafkaToFF"))
  @Parameters(Array("sourceFile"))
  def runMLBatchJob(sourceFile: String): Unit = {
    // run batch job.
    Classification.runBatch(sourceFile)
    println(" ---------> ML batch job succeeded <------------")
  }

  @Test(dependsOnMethods = Array("runMLBatchJob"))
  @Parameters(Array("broker", "topic", "sourceFile"))
  def de_init(broker: String, topic: String, sourceFile: String): Unit = {
    // destroy kafka topic created
    import scala.collection.JavaConversions._
    import java.io.File
    val props = new Properties
    props.put("bootstrap.servers", broker)
    val admin = AdminClient.create(props)
    val result = admin.deleteTopics(List(topic))
    result.all().get
    // delete the sourceFile as well.
    val srcFile = new File(sourceFile)
    srcFile.delete
    // post this print the end of successful completion
    println(" ----------> De-init successful <-------------")
  }

}