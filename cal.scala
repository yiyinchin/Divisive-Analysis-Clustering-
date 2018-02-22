import org.apache.spark.sql.SparkSession
import Array._

/**
  * Created by chinyiy on 11/12/17.
  */
object DIANA {


  /**
    * key of the row with the highest Average Dissimilarity to the other objects
    *
    * @param x dissimilarity matrix with key
    * @return the key of the row with larges non-negative sum
    *         returns -1 if the largest sum is less than zero
    */


  def keyRMAD(
             x: Array[(Int, Array[Double])]
             ): Int ={
    val m = x.length // maybe (x.count).toInt if the matrix is a RDD

    val inStartTime = System.nanoTime()

    //Average Sum of the elements in the row of matrix
    val aveSumRow = x.map {
      case (i, value) => (value.sum / (m - 1), i)
    }

    //Get the max value and key
    val maxAveSum = aveSumRow.max

    if(maxAveSum._1 < 0) return -1

    //Get the key of the highest average dissimilarity to all other objects
    val key = maxAveSum._2

    val inTimeSeconds = (System.nanoTime() - inStartTime) / 1e9

    key
  }

  /**
    * Average dissimilarity with the remaining objecy compare with the objects of the splinter group
    *
    * @param remainGroup Dissimilarity matrix of the remaining objects
    * @param splinterGroup Splinter group
    * @return Key of the largest positive difference
    */

  def diffAD(
            remainGroup: Array[(Int, Array[Double])],
            splinterGroup: Array[Array[Double]]
            ): Int ={
    val m = remainGroup.length  //maybe (x.count).toInt if it is RDD
    val n = splinterGroup(1).length

    //Average Sum of the elements in the remain dissimilarity matrix

    val aveSumRemain = remainGroup.map{
      case (key, value) => (key, value.sum/(m-1))
    }

    //Average Sum of the elements in the splinter group

    val aveSumSplinter = splinterGroup.map{
      case (value) => value.sum/n
    }

    //Combine 2 Groups together

    val combine = aveSumRemain.zip(aveSumSplinter) // if is not working then (aveSumRemain.collect).zip.(aveSumSplinter)

    //difference between the Sum of the Remaining objects of each rows and the elements from the splinter group
    val diffSum = combine.map{
      case ((key, valueR), valueS) => (valueR-valueS, key)
    }

    val maxDiffSum = diffSum.max

    //Key of the largest positive difference

    if(maxDiffSum._1 < 0) return -1 else maxDiffSum._2

  }


  /**
    * Dissimilarity of the Remaining Objects
    *
    * @param x dissimilarity matrix with a key
    * @param key key of the highest average dissimilarity
    * @return dissimilarity matrix of the remaining objects
    */

  def objRemains(
                x: Array[(Int, Array[Double])],
                key: Int
                ): Array[(Int, Array[Double])] ={

    val reduce1 = x.filter{
      case (i, value) => i != key
    }

    val buf = reduce1.map{
      case (i, value) => (i, value.toBuffer)
    }

    val remains = buf.map{
      case (i, value) => value.remove(key, 1); (i, value.toArray)
    }

    remains
  }

  /**
    *
    *
    * @param keyedMat
    * @param key
    * @param AllKey
    * @return
    */

  def objSplinter(
               keyedMat: Array[(Int,Array[Double])],
               key: Array[Int],
               AllKey: Array[Int] // need to find a way to get this
               ): Array[Array[Double]] ={

    // The remain keys(indexes)
    val remainKeys = AllKey diff key // need to fix this, not working?

    val splinterObj = keyedMat.filter{ case (i, value) => remainKeys.exists(_==i)}

    val splinter = splinterObj.map{ case(i, value) => key map value}

    splinter

  }

  /**
    * Select the cluster with the largest diameter
    *
    * @param fullMatrix
    * @param remainGroup
    * @param splinterkeys
    * @return the cluster with the largest diameter
    */

  def largestDiam(
                 fullMatrix: Array[(Int, Array[Double])], //full matrix
                 remainGroup: Array[(Int, Array[Double])],
                 splinterkeys: Array[Int]
                 ): Array[(Int, Array[Double])] ={

    // make splinter group from matrix

    // select the splinter group according to the splinter keys
    val splinterRows = fullMatrix.filter{ case (i, value) => splinterkeys.exists(_==i)}

    // only select the splinter index for the matrix
    val splinterGroup: Array[(Int, Array[Double])] = splinterRows.map{ case(i, value) => (i, splinterkeys map value) }

    // find the maximum "diameters" from each groups

    val maxSplinter = splinterGroup.map{ case(i, value) => value.max }

    val maxRemain = remainGroup.map{ case(i, value) => value.max }

    // compare the chosen diameters from the groups
    val splinterMax = maxSplinter.collect.max

    val remainMax = maxRemain.collect.max

    // chose the largest or the bigger diameter group

    if(splinterMax > remainMax) return splinterGroup else remainGroup

  }



  def main(
          args: Array[String]
          ): Unit ={

    if(args.length < 1){
      System.err.println("Usage: DIANA <file>")
      System.exit(1)
    }

    //Create spark session
    val spark = SparkSession
      .builder
      .appName("Diana")
      .getOrCreate()

    val sc = spark.sparkContext

    //load the data
    val data = spark.read.format("csv").load(args(0))

    val dataC = data.collect()

    //Returns the number of rows in the Dataset
    val numRows = data.count().toInt

    val longNum = numRows.toLong

    //Defining a two dimensional array
    var myMatrix = ofDim[Double](numRows, numRows)

    //build a Matrix
    for(i <- 0 until numRows){
      for(j <- 0 until numRows){
        myMatrix(i)(j) = dataC(i).getString(j).toDouble
      }
    }

    val paraKey = sc.parallelize((0 until numRows))

    val paraMat = sc.parallelize(myMatrix.toSeq)

    val keyedMat = paraKey.zip(paraMat)

  }



}

var splinter2 = ofDim[Double](sizeM-2, 2)

var p = 0
for (i <- 0 until sizeM){
    if(i != index1 && i != index2){
      print(i)
      var q = 0
         for(j <- 0 until sizeM){
           if(j == index1 || j == index2){
                print(j)
             splinter2(p)(q) = (myMatrix(i))(j)
             q += 1
           }
         }
       p += 1
    }
}
