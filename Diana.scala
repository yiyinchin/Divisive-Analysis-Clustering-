package org.apache.spark.mllib.clustering

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.annotation.Since
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.ml.clustering.{KMeans => NewKMeans}
import org.apache.spark.ml.util.Instrumentation
import org.apache.spark.mllib.Distance
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.BLAS.{axpy, scal}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils
import org.apache.spark.util.random.XORShiftRandom

class DianaAlgorithm private(
                   private var initializationMode: String
                   ) extends Serializable with Logging {

}

object DianaAlgorithm {

  /**
    * Returns the Manhattan distance (The diameter of the cluster)
    */

  private[clustering] def ManhattanDistance(
                                           v1: VectorWithNorm,
                                           v2: VectorWithNorm
                                           ): Double = {
    Distance.absVal(v1.vector, v2.vector)
  }
}
