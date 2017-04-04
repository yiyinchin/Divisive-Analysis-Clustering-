package org.apache.spark.mllib.clustering

import scala.collection.JavaConverters._

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Since
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.pmml.PMMLExportable
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

class DianaAlgorithmModel(val clustered: Array[Vector])
  extends Saveable with Serializable with PMMLExportable {

  private val clusteredWithNorm =
    if(clustered == null) null else clustered.map(new VectorWithNorm(_))

  /**
    * A Java-friendly constructor that takes an Iterable of Vectors.
    */

  def this(centers: java.lang.Iterable[Vector]) = this(centers.asScala.toArray)



}
