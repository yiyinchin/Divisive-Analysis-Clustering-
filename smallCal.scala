package org.apache.spark.mllib

import org.apache.spark.internal.Logging
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, IndexedRow, MatrixEntry}
import org.apache.spark.rdd.RDD

object smallCal extends Logging {

  /**
    * Average dissimilarity to the Other Objects
    *
    */

  def averageDissOO (
                    p: RDD[MatrixEntry]
                    ): Array[Double] ={
    //Create a Coordinate Matrix from an RDD[MatrixEntry]
    val x: CoordinateMatrix = new CoordinateMatrix(p)

    // Get its size
    val m = x.numRows()
    val n = x.numCols()

    // Sum of the elements in the row of matrix
    val sumRow = x.toIndexedRowMatrix.rows.map{
      case IndexedRow(i, value) => (value.toArray.sum)
    }

    // Find the average of dissimilarity
    val aveRow = sumRow.map{
      case (value) => (value / (m-1))
    }
    
    //TODO find the largest non negative average value
    
  }
  
    /**
    * Average dissimilarity to Objects of Splinter Group
    */

  def averageDissSG(
                   x: CoordinateMatrix
                   ): Double ={
    // Choose the splinter group row only
    0.0
  }
  
  /**
    * Difference between average dissimilarity to Objects and average dissimilarity to Objects of Splinter Group
    *
    * @param x Average Dissimilarity to Remaining Objects
    * @param y Average Dissimilarity to Objects to Splinter Group
    * @return the difference 
    */

  def diff(
          x: Array[Double],
          y: Array[Double]
          ): Double ={

    val d: Array[Double]

    var i = 0
    while(i < x.length){
      d(i) = x(i) - y(i)
      i += 1
    }

    // TODO Find the largest non negative average value
    val p = d.max
    p
  } 

}
