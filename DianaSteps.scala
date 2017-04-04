package org.apache.spark.mllib

import breeze.numerics.{abs, sqrt}
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.BLAS.dot
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, MatrixEntry, RowMatrix}
import org.apache.spark.rdd.RDD

object Distance extends Logging {

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

  /**
    * A RowMatrix is a row-orientated distributed matrix without meaningful row indices, backed
    * by an RDD of its rows, where each row is a local vector. Since each row is represented by
    * a local vector, the number of columns is limited by the integer range but it should be
    * much smaller in practice.
    */

  def dissimilarityMatrix(
                           x: RDD[Vector],
                           n: Int,
                           p: Int
                         ): RowMatrix ={
    val jtmd = Array(0)
    val valMd = Array(0)
    val ndyst = 1

    val len = 1 + ( n * (n - 1)/2)
    // Should be working ? http://stackoverflow.com/questions/9413507/scala-creating-a-type-parametrized-array-of-specified-length
    val dys =  new Array[Double](len)  // TODO you are a problem len =/= length, size problem

    var nlk = 1 // index of the point of the next element in the matrix
    val pp = p

    for(l <- 2 to n ){//Putting dissimilarities in new array
      val lSub = l - 1

      for(k <- 1 to lSub){ // Going down the data matrix
        var clk = 0.0 // Sum of distances in row
        nlk = nlk + 1
        var npres = 0 // counting how many in the row

        for(j <- 1 to p){ // Going across the columns

          // Sum the distances in this column
          // See if any value is NA in this row
          // if there is, go to next value

          // Check if any in this row are missing
          if(jtmd.length > 0 && jtmd(j) < 0){
            // Some x(*, j) are missing (are == NA)
            // valMd(j) == max value * 1.1 for column (but really whole matrix's max)

            if(x(l,j) == valMd(j) || x(k,j) == valMd(j)){
              println("NA\n")
              //TODO next
            }
          }

          // Number of valid values (maybe == row * col - num_of_NA?)
          npres = npres + 1

          val dis = x(l,j) - x(k,j)
          if(ndyst == 1){
            // Euclidean Metric
            clk = clk + (dis * dis)
          } else {
            // Manhattan Metric
            clk = clk + abs(dis)
          }
        }

        var rPres = npres

        if(npres == 0){
          // Error: notify calling C code of error (raise exception, old-school style)
          println("Error: npres == 0\n")
          val jhalt = 1
          dys(nlk) = -1.0
        } else {
         if(ndyst == 1){
           // Euclidean
           dys(nlk) = sqrt( clk * (pp / rPres))
         } else {
           // Manhattan
           dys(nlk) = clk * (pp / rPres)
         }
        }
      }
    }

    /**
      * An array of dissimilarities (needs to be made 2D)
      * note, first element is 0.0, so this can be discarded
      */
    dys(len-1)
  }


}

