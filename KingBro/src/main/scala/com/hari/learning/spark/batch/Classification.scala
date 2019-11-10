package com.hari.learning.spark.batch

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.ml.feature.{ RegexTokenizer, HashingTF }
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.{ SparkSession, Row }
import org.apache.spark.sql.types.{ StructField, StructType, StringType, IntegerType }
import org.apache.spark.sql.functions.udf

/**
 * Classifying the Ticket data staged in "tickets.json" using Logistics Regression
 * We will classify the tickets based on description values if it contains
 * 'critical' (could be extended for set of values)
 * Do not have any experience with ML packages , but giving it a try :).
 *
 * @author harikrishna
 */

class Classification {

  // defining a simple binary classifying logic , classifying
  def run(src: String): Unit = {
    // running it in local mode for testing
    val spark = SparkSession.builder.master("local[*]").appName("EntityClassification").getOrCreate
    import spark.implicits._
    def isCritical: (Boolean => Int) = bool => if (bool) 1 else 0
    def criticalUdf = udf(isCritical)
    val cachedTickets = spark.read.json(src).withColumn("label", criticalUdf('desc.like("%critical%"))).cache
    val Array(trainingData, testData) = cachedTickets.randomSplit(Array(0.70, 0.30))

    // build models with trained data and test data.
    val regTokenizer = new RegexTokenizer().setInputCol("desc").setOutputCol("content")
    val hashTF = new HashingTF().setInputCol(regTokenizer.getOutputCol).setOutputCol("features").setNumFeatures(1000)
    val logReg = new LogisticRegression().setMaxIter(40).setRegParam(0.01)
    // build pipeline
    val pipe = new Pipeline().setStages(Array(regTokenizer, hashTF, logReg))
    val genModel = pipe.fit(trainingData)
    val trainedPreds = genModel.transform(trainingData)
    val testPreds = genModel.transform(testData)
    // try printing the data and test it out.
    println("Printing the trained data")
    trainedPreds.show
    println("Printing the test data")
    testPreds.show
  }
}

object Classification {

  def runBatch(srcFile: String): Unit = new Classification().run(srcFile)
}