import org.apache.spark.sql.SparkSession
import Array._
import scala.collection.mutable.ArrayBuffer

object DIANA {


  /**
    * key of the row with the highest Average Dissimilarity to the other objects
    *
    * @param keyedMat dissimilarity matrix with key
    * @return the key of the row with larges non-negative sum
    *         returns -1 if the largest sum is less than zero
    */


  def keyRMAD(
             keyedMat: org.apache.spark.rdd.RDD[(Int, Array[Double])]
             ): Int ={
    val m = (keyedMat.count).toInt
    //val m = x.length // maybe (x.count).toInt if the matrix is a RDD

    val inStartTime = System.nanoTime()

    //Average Sum of the elements in the row of matrix
    val aveSumRow = keyedMat.map { case (i, value) => (value.sum / (m - 1), i)}

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
            remainGroup: org.apache.spark.rdd.RDD[(Int, Array[Double])],
            splinterGroup: org.apache.spark.rdd.RDD[Array[Double]]
            ): Int ={
    val m = (remainGroup.count).toInt
    val splint = splinterGroup.collect
    val n = splint(1).length

    //Average Sum of the elements in the remain dissimilarity matrix

    val aveSumRemain = remainGroup.map{ case (key, value) => (key, value.sum/(m-1))}

    //Average Sum of the elements in the splinter group

    val aveSumSplinter = splinterGroup.map{ case (value) => value.sum/n }

    //Combine 2 Groups together

    val combine = aveSumRemain.zip(aveSumSplinter) 

    //difference between the Sum of the Remaining objects of each rows and the elements from the splinter group
    val diffSum = combine.map{ case ((key, valueR), valueS) => (valueR-valueS, key)}

    val maxDiffSum = diffSum.max

    //Key of the largest positive difference

    if(maxDiffSum._1 < 0) return -1 else maxDiffSum._2

  } 

  /**
    * Dissimilarity of the remaining objects
    *
    * @param keyedmat dissimilarity matrix with a key
    * @param keyA Array of the key of the highest average dissimilarity
    * @param allKey Array of all of the keys
    * @return dissimilarity matrix of the remaining objects
    */

  def objRemains(
                keyedmat:org.apache.spark.rdd.RDD[(Int, Array[Double])],
                keyA: Array[Int],
                allKey: Array[Int]
                ): org.apache.spark.rdd.RDD[(Int, Array[Double])] ={

    val remainKeys = allKey diff keyA

    val keyedValueRemove = keyedmat.map{ case(i, value) => (i, remainKeys map value)}

    val remains = keyedValueRemove.filter{ case(i, value) => remainKeys.exists(_==i)}

    remains
  }

  /**
    * Dissimilarity of the Splinter Group
    *
    * @param keyedMat full dissimilarity matrix with a key
    * @param key key of the highest average dissimilarity
    * @param AllKey all of the keys (indexes)
    * @return objects of splinter group
    */

  def objSplinter(
               keyedMat:org.apache.spark.rdd.RDD[(Int, Array[Double])],
               key: Array[Int],
               AllKey: Array[Int]
               ): org.apache.spark.rdd.RDD[Array[Double]] ={

    // The remain keys(indexes)
    val remainKeys = AllKey diff key

    //filter out the remaining keys
    val splinterObj = keyedMat.filter{ case (i, value) => remainKeys.exists(_==i)}

    //from the filtered group, extract the objects of the splinter group
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
    **/

  def largestDiam(
                 fullMatrix: org.apache.spark.rdd.RDD[(Int, Array[Double])], //full matrix
                 remainGroup: org.apache.spark.rdd.RDD[(Int, Array[Double])],
                 splinterkeys: Array[Int]
                 ): org.apache.spark.rdd.RDD[(Int, Array[Double])] ={

    // make splinter group from matrix

    // select the splinter group according to the splinter keys
    val splinterRows = fullMatrix.filter{ case (i, value) => splinterkeys.exists(_==i)}

    // only select the splinter index for the matrix
    val splinterGroup = splinterRows.map{ case(i, value) => (i, splinterkeys map value) }

    // find the maximum "diameters" from each groups

    val maxSplinter = splinterGroup.map{ case(i, value) => value.max }

    val maxRemain = remainGroup.map{ case(i, value) => value.max }

    // compare the chosen diameters from the groups
    val splinterMax = maxSplinter.max

    val remainMax = maxRemain.max

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

    // Key are equal to the indexes
    val paraKey = sc.parallelize((0 until numRows))

    val paraMat = sc.parallelize(myMatrix.toSeq)

    val keyedMat = paraKey.zip(paraMat)

    val largeKey = keyRMAD(keyedMat)

    println("The row with the largest Sum: " + largeKey) 

    val allofKey = keyedMat.map{ case(i, key) => i }

    val allKey = allofKey.collect
    
    var keyA : ArrayBuffer[Int] = ArrayBuffer()

    keyA += largeKey

    val aKey =  keyA.toArray

    val objRem1 = objRemains(keyedMat, aKey, allKey)

    val objSpl1 = objSplinter(keyedMat, aKey, allKey)

    val aveDiff = diffAD(objRem1, objSpl1)
  
    println("The second largest Sum: " + aveDiff)

  }



}
