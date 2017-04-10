package org.apache.spark.mllib

import org.apache.spark.internal.Logging
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, IndexedRow}
import org.apache.spark.rdd.RDD

object smallCal extends Logging {

  /**
    * index of the row Max Average dissimilarity to the Other Objects
    * @return the index of row with largest non-negative sum
    *         returns -1 if the largest sum is less than zero
    */

  def indexRMAD (
                  x: CoordinateMatrix
                ): Int ={
    // Get its size
    val m = x.numRows()
    val n = x.numCols()

    // Average Sum of the elements in the row of matrix
    val aveSumRow = x.toIndexedRowMatrix.rows.map{
      case IndexedRow(i, value) => value.toArray.sum / (m-1)
    }

    val aveSumRowArray = aveSumRow.collect()

    val maxAveRowSum = aveSumRowArray.max

    if(maxAveRowSum < 0) return -1

    aveSumRowArray.indexOf(maxAveRowSum)
  }

  def diffAD(
            x: CoordinateMatrix,
            y: CoordinateMatrix
            ): Int ={
    //Get its size
    val m = x.numRows()
    val n = x.numCols()
    val p = y.numRows()
    val q = y.numCols()

    // Average Sum of the elements in the row of matrix
    val aveSumRow = x.toIndexedRowMatrix.rows.map{
      case IndexedRow(i, value) => value.toArray.sum / (m - 1)
    }

    //Average Sum of the elements in the Splinter Group
    val aveSumSplinter = y.toIndexedRowMatrix.rows.map{
      case IndexedRow(i, value) => value.toArray.sum / q
    }

    //difference betweeen the Sum of the row of matrix and the elements from the Splinter Group
    val diffSum = aveSumRow.zip(aveSumSplinter).map{
      case (u,v) => u-v
    }

    val diffSumArray = diffSum.collect()

    val maxDiffSum = diffSumArray.max

    if(maxDiffSum < 0) return -1

    diffSumArray.indexOf(maxDiffSum)
  }
  
   /**
    * Returns the manhattan distance between two vectors.
    *
    * The manhattan distance is used to measure the diameter of a cluster with the largest diameter.
    * @param v1 first Vector
    * @param v2 Second Vector
    * @return distance between two Vectors
    */

  def absVal(v1: Vector, v2: Vector): Double ={
    require(v1.size == v2.size, s"Vector dimensions do not match: Dim(v1) = ${v1.size} and Dim(v2)" +
      s"=${v2.size}.")
    var distance = 0.0
    (v1, v2) match {
      case (v1: SparseVector, v2: SparseVector) =>
        val v1Values = v1.values
        val v1Indices = v1.indices
        val v2Values = v2.values
        val v2Indices = v2.indices
        val nnzv1 = v1Indices.length
        val nnzv2 = v2Indices.length

        var kv1 = 0
        var kv2 = 0
        while (kv1 < nnzv1 || kv2 < nnzv2) {
          var score = 0.0

          if(kv2 >= nnzv2 || (kv1 < nnzv1 && v1Indices(kv1) < v2Indices(kv2))) {
            score = v1Values(kv1)
            kv1 += 1
          } else if (kv1 >= nnzv1 || (kv2 < nnzv2 && v2Indices(kv2) < v1Indices(kv1))){
            score = v2Values(kv2)
            kv2 += 1
          } else {
            score = v1Values(kv1) - v2Values(kv2)
            kv1 += 1
            kv2 += 1
          }
          distance += math.abs(score)
        }

      case(v1: SparseVector, v2: DenseVector) =>
        distance = absVal(v1, v2)

      case(v1: DenseVector, v2: SparseVector) =>
        distance = absVal(v2, v1)

      case(DenseVector(vv1), DenseVector(vv2)) =>
        var kv = 1
        val sz = vv1.length
        while(kv < sz) {
          val score = vv1(kv) - vv2(kv)
          distance += math.abs(score)
          kv += 1
        }
      case _ =>
        throw new IllegalArgumentException("Do not support vector type" + v1.getClass +
          " and " + v2.getClass)
    }
    math.abs(distance)
  }
}
