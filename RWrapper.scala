package org.apache.spark.ml.r

import org.apache.hadoop.fs.Path
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkException
import org.apache.spark.ml.util.MLReader


private[r] object RWrappers extends MLReader[Object]{
  
  override def load(path: String): Object ={
    implicit val format = DefaultFormats
    val rMetadataPath = new Path(path, "rMetadata").toString
    val rMetadataStr = sc.textFile(rMetadataPath, 1).first()
    val rMetadata = parse(rMetadataStr)
    val className = (rMetadata \ "class").extract[String]
    className match {
      case "org.apache.spark.ml.r.BernoulliWrapper" => BernoulliWrapper.load(path)
    }
  }
}