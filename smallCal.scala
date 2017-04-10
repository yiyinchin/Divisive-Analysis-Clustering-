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
}
