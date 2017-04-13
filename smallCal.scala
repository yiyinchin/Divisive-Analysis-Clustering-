package org.apache.spark.mllib

import org.apache.spark.internal.Logging
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.rdd.RDD

object smallCal extends Logging {

  /**
    * index of the row Max Average dissimilarity to the Other Objects
    * @return the index of row with largest non-negative sum
    *         returns -1 if the largest sum is less than zero
    */

  def indexRMAD(
                 x: IndexedRowMatrix
               ): Int ={
    // Get its size
    val m = x.numRows()
    val n = x.numCols()

    require(m == n)

    // Average Sum of the elements in the row of matrix
    val aveSumRow = x.rows.map{
      case IndexedRow(i, value) => (i, value.toArray.sum / (m-1))
    }

    val aveSumRowArray = aveSumRow.collect()

    val maxAveRowSum:(Long, Double)= aveSumRowArray.max

    //if(maxAveRowSum < 0) return -1

    aveSumRowArray.indexOf(maxAveRowSum)
  }
  def diffAD(
            x: IndexedRowMatrix,
            y: IndexedRowMatrix
            ): Int ={
    //Get its size
    val m = x.numRows()
    val n = x.numCols()
    val p = y.numRows()
    val q = y.numCols()

    // Average Sum of the elements in the row of matrix
    val aveSumRow = x.rows.map{
      case IndexedRow(i, value) => value.toArray.sum / (m - 1)
    }

    //Average Sum of the elements in the Splinter Group
    val aveSumSplinter = y.rows.map{
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
  
  // Local Matrix, dense Matrix
import org.apache.spark.mllib.linalg.{Vectors, Matrices}

val rows = sc.parallelize(Seq(
 (0L, Array(0.0,2.0,6.0,10.0,9.0)),
 (0L, Array(2.0,0.0,5.0,9.0,8.0)),
 (0L, Array(6.0,5.0,0.0,4.0,5.0)),
 (0L, Array(10.0,9.0,4.0,0.0,3.0)),
 (0L, Array(9.0,8.0,5.0,3.0,0.0)))
).map{ case (i, xs) => IndexedRow(i, Vectors.dense(xs))}

val indexedRowMatrix = new IndexedRowMatrix(rows,5L,5)

}
