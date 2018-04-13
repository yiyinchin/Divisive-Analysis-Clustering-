import org.apache.spark.sql.SparkSession
import scala.runtime.ScalaRunTime._
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
             ): Int = {
    val m = (keyedMat.count).toInt
    //val m = x.length // maybe (x.count).toInt if the matrix is a RDD

    val inStartTime = System.nanoTime()

    //Average Sum of the elements in the row of matrix
    val aveSumRow = keyedMat.map { case (i, value) => (value.sum / (m - 1), i) }

    //Get the max value and key
    val maxAveSum = (aveSumRow.collect).maxBy(x => x._1)

    if (maxAveSum._1 < 0) return -1

    //Get the key of the highest average dissimilarity to all other objects
    val key = maxAveSum._2

    val inTimeSeconds = (System.nanoTime() - inStartTime) / 1e9

    key
  }

  /**
    * Average dissimilarity with the remaining objecy compare with the objects of the splinter group
    *
    * @param remainGroup   Dissimilarity matrix of the remaining objects
    * @param splinterGroup Splinter group
    * @return Key of the largest positive difference
    */

  def diffAD(
              remainGroup: org.apache.spark.rdd.RDD[(Int, Array[Double])],
              splinterGroup: org.apache.spark.rdd.RDD[Array[Double]]
            ): Int = {
    val m = (remainGroup.count).toInt
    val splint = splinterGroup.collect
    val n = splint(0).length

    //val n = splint(0).length = = which is which

    //Average Sum of the elements in the remain dissimilarity matrix

    val aveSumRemain = remainGroup.map { case (key, value) => (key, value.sum / (m - 1)) }

    //Average Sum of the elements in the splinter group

    val aveSumSplinter = splinterGroup.map { case (value) => value.sum / n }

    //Combine 2 Groups together

    val combine = aveSumRemain.zip(aveSumSplinter)

    //difference between the Sum of the Remaining objects of each rows and the elements from the splinter group
    val diffSum = combine.map { case ((key, valueR), valueS) => (valueR - valueS, key) }

    val maxDiffSum = (diffSum.collect).maxBy(x => x._1)

    //Key of the largest positive difference

    if (maxDiffSum._1 <= 0) {
      -1
    } else if (m == 1) {
      val remainKey = remainGroup.map { case (i, value) => i }
      val rK = remainKey.collect
      rK(0)
    } else maxDiffSum._2

  }

  /**
    * Dissimilarity of the remaining objects
    *
    * @param keyedmat dissimilarity matrix with a key
    * @param keyA     Array of the key of the highest average dissimilarity
    * @param allKey   Array of all of the keys
    * @return dissimilarity matrix of the remaining objects
    */

  def objRemains(
                  keyedmat: org.apache.spark.rdd.RDD[(Int, Array[Double])],
                  keyA: Array[Int],
                  allKey: Array[Int]
                ): org.apache.spark.rdd.RDD[(Int, Array[Double])] = {

    val remainKeys = allKey diff keyA

    val keyedValueRemove = keyedmat.map { case (i, value) => (i, remainKeys map value) }

    val remains = keyedValueRemove.filter { case (i, value) => remainKeys.exists(_ == i) }

    remains
  }

  /**
    * Dissimilarity of the Splinter Group
    *
    * @param keyedMat full dissimilarity matrix with a key
    * @param key      key of the highest average dissimilarity
    * @param AllKey   all of the keys (indexes)
    * @return objects of splinter group
    */

  def objSplinter(
                   keyedMat: org.apache.spark.rdd.RDD[(Int, Array[Double])],
                   key: Array[Int],
                   AllKey: Array[Int]
                 ): org.apache.spark.rdd.RDD[Array[Double]] = {

    // The remain keys(indexes)
    val remainKeys = AllKey diff key

    //filter out the remaining keys
    val splinterObj = keyedMat.filter { case (i, value) => remainKeys.exists(_ == i) }

    //from the filtered group, extract the objects of the splinter group
    val splinter = splinterObj.map { case (i, value) => key map value }

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
                 ): org.apache.spark.rdd.RDD[(Int, Array[Double])] = {

    // make splinter group from matrix

    // select the splinter group according to the splinter keys
    val splinterRows = fullMatrix.filter { case (i, value) => splinterkeys.exists(_ == i) }

    // only select the splinter index for the matrix
    val splinterGroup = splinterRows.map { case (i, value) => (i, splinterkeys map value) }

    // find the maximum "diameters" from each groups

    val maxSplinter = splinterGroup.map { case (i, value) => value.max }

    val maxRemain = remainGroup.map { case (i, value) => value.max }

    // compare the chosen diameters from the groups
    val splinterMax = maxSplinter.max

    val remainMax = maxRemain.max

    // chose the largest or the bigger diameter group

    if (splinterMax > remainMax) return splinterGroup else remainGroup

  }

  /**
    * Gives you the dissimilarity matrix of the remaining group with indexes
    *
    * @param splinterKeys indexes of the splinter group
    * @param allKeys      all of the keys (all indexes)
    * @param fullMatrix   full dissimilarity matrix with a key
    * @return dissimilarity matrix of the remaining group
    */

  def groups(
              splinterKeys: Array[Int],
              allKeys: Array[Int],
              fullMatrix: org.apache.spark.rdd.RDD[(Int, Array[Double])]
            ): org.apache.spark.rdd.RDD[(Int, Array[Double])] = {

    val remainKeys = allKeys diff splinterKeys

    val remainRows = fullMatrix.filter { case (i, value) => remainKeys.exists(_ == i) }

    val remainClust = remainRows.map { case (i, value) => (i, splinterKeys map value) }

    remainClust
  }

  /**
    * Find the height of the clusters only can be applied when the clusters only left 2 elements
    *
    * @param remainGroup Remaining group, with key
    * @return height
    */

  def diameter(
                remainGroup: org.apache.spark.rdd.RDD[(Int, Array[Double])]
              ): Double = {

    val maxDia = remainGroup.map { case (i, value) => value.max }

    val diaMax = maxDia.max

    diaMax
  }

  /**
    * Find the height of the clusters (singleton)
    *
    * @param splinterGroup Splinter group, two dimension
    * @param remainGroup   Remaining group, with key
    * @return Height
    */


  def height(
              splinterGroup: org.apache.spark.rdd.RDD[Array[Double]],
              remainGroup: org.apache.spark.rdd.RDD[(Int, Array[Double])]
            ): Double = {

    // find the maximum "diameters" from each groups

    val maxSplinter = splinterGroup.map { case (value) => value.max }

    val maxRemain = remainGroup.map { case (i, value) => value.max }

    // compare the chosen diameters from the groups
    val splinterMax = maxSplinter.max

    val remainMax = maxRemain.max

    // chose the largest or the bigger diameter group

    if (splinterMax > remainMax) return splinterMax else remainMax

  }
  
  /**
    * Divisive coefficient
    *
    * @param heights The numbers of when it splits
    * @return Divisive coefficient
    */

  def divisiveCoef(
                  heights : Array[Double]
                  ): Double ={
    //Find the first height of the first element
    if(heights(0) == 0.0){
      heights.update(0, heights(1))
    }

    //Compare the current element to the left, pick the lower height
    val size = heights.length
    val heightsCurrent = heights.clone
    for(a <- 1 until size){
      if(heightsCurrent(a) < heights(a -1)){
        heightsCurrent.update(a, heights(a-1))
      }
    }

    //Find the maximum height
    val maxHeight = heightsCurrent.max

    //1 - the division of each element of the heights with the maximum height
    val dc = heightsCurrent.map{ case (i) => 1 - (i/maxHeight)}

    val sumDC = dc.sum

    val aveDC = sumDC/size

    aveDC
  }  

  def main(
            args: Array[String]
          ): Unit = {

    if (args.length < 1) {
      System.err.println("Usage: DIANA <file>")
      System.exit(1)
    }

    val session = SparkSession.builder().appName("test").master("local").getOrCreate()

    val data = session.read.format("csv").load(args(0))

    val dataC = data.collect()

    //Returns the number of rows in the Dataset
    val numRows = data.count().toInt

    val longNum = numRows.toLong

    //Defining a two dimensional array
    var myMatrix = ofDim[Double](numRows, numRows)

    //build a Matrix
    for (i <- 0 until numRows) {
      for (j <- 0 until numRows) {
        myMatrix(i)(j) = dataC(i).getString(j).toDouble
      }
    }

    // Key are equal to the indexes
    val paraKey = sc.parallelize((0 until numRows))

    val paraMat = sc.parallelize(myMatrix.toSeq)

    //Matrix with Indexes
    val keyedMat = paraKey.zip(paraMat)

    val allKey = (keyedMat.map { case (i, key) => i }).collect
    // The splinter indexes (ArrayBuffer)
    var keyA: ArrayBuffer[Int] = ArrayBuffer()
    // The splinter indexes (Array)
    var splinterKeys: Array[Int] = Array(0)
    // The input indexes for the next clustering
    var inputKey = allKey
    // largest positive splinter from the cluster
    var splinterIndex = 0
    //
    var end = allKey.length
    //
    var found = 1
    //The matrix for clustering, the next cluster to split
    var largeDiam = keyedMat
    //Size of the matrix
    var size = 0
    // The Height
    var diameters = 0.0
    // The remaining indexes
    val stack = new scala.collection.mutable.Stack[Array[Int]]
    // The size of the remaining cluster
    var sizeR = 0
    // The order of the splinters
    var clustOrder : ArrayBuffer[Array[Int]] = ArrayBuffer()
    // The secret sorting order of the splinters
    var secOrder : ArrayBuffer[Array[Int]] = ArrayBuffer()
    // The heights
    val lengthClust = allKey.length
    var clustHeight: ArrayBuffer[Double] = ArrayBuffer()

    for (i <- 1 to lengthClust) {
      clustHeight += 0.0
    }
    val clustHei = clustHeight.toArray

    while (found < end) {
      //while number of clusters is less than the number of clusters at the end
      //LargeDiam is the cluster with the largest Diameter, so the next to be split

      size = ((largeDiam.map { case (i, value) => i }).collect).length
      println("Size: " + size)

      if (size == 2) {

        diameters = diameter(largeDiam)

        val keys = (largeDiam.map { case (i, value) => i }).collect

        println("Height between " + stringOf(keys) + " is " + diameters)

        val molly = secOrder.exists(_.sameElements(keys))
        val soughtObject = secOrder.filter(_.sameElements(keys))
        val indexOldSplinters = secOrder.indexOf(soughtObject(0))

        var firstElement = (ArrayBuffer(keys(0))).toArray
        var secondElement = (ArrayBuffer(keys(1))).toArray

        if(molly){ // if (molly == true)
          if(firstElement.min < secondElement.min) {
            clustOrder.update(indexOldSplinters, firstElement)
            clustOrder.insert(indexOldSplinters + 1, secondElement)
            secOrder.update(indexOldSplinters, firstElement)
            secOrder.insert(indexOldSplinters + 1, secondElement)
          }
          else{
            clustOrder.update(indexOldSplinters, secondElement)
            clustOrder.insert(indexOldSplinters + 1, firstElement)
            secOrder.update(indexOldSplinters, secondElement)
            secOrder.insert(indexOldSplinters + 1, firstElement)
          }
        }
        val lengthLeft = (clustOrder.map{ case(i) => i.length}).toArray
        println("Lengthsssss: " + stringOf(lengthLeft))
        val cosmos = (lengthLeft.take(indexOldSplinters+1)).sum
        clustHei.update(cosmos, diameters)
        println("COSMOS?!: " + stringOf(cosmos))

        println("Change of the orders: " + stringOf(clustOrder))
        println("Banner: " + stringOf(clustHei))

        //get the remaining heights of the clusters

        if (stack.isEmpty) {

        } else {
          val reKeys = stack.pop
          val remainA = keyedMat.filter { case (i, value) => reKeys.exists(_ == i) }
          val remainClustA = remainA.map { case (i, value) => (i, reKeys map value) }

          largeDiam = remainClustA
          inputKey = (remainClustA.map { case (i, value) => i }).collect
        }

      }
      else {
        //Find the splinter element from the cluster
        splinterIndex = keyRMAD(largeDiam)
        keyA += splinterIndex
        splinterKeys = keyA.toArray

        while (splinterIndex != -1) {
          // Find the rest of the splinter group from the cluster

          val splintObj = objSplinter(keyedMat, splinterKeys, inputKey)
          val remainObj = objRemains(keyedMat, splinterKeys, inputKey)
          splinterIndex = diffAD(remainObj, splintObj) // largest positive splinter from the cluster

          if (splinterIndex != -1) {
            // add to the set of splinter keys
            keyA += splinterIndex
            splinterKeys = keyA.toArray
          }
          else {
            // -1, so no more splinter elements from this cluster
            // record current split and set up next splinter

            //save the remaining group of the cluster and record the current height
            val remain = groups(splinterKeys, inputKey, keyedMat)
            val height1 = height(splintObj, remainObj) //TODO record this outside the loop

            //Find the next initial cluster
            val inputKeyPrev = inputKey

            println("Whats the input key 1: " + stringOf(inputKeyPrev))

            val keyS = splinterKeys//get the splinterIndex groups
            val keyR = inputKey diff keyS // get the remain groups
            val sKey = keyS.sorted
            val sLength = keyS.length
            val rLength = keyR.length

            println("The height of: " + stringOf(keyR) + " & " + stringOf(keyS) + " is " + stringOf(height1))

            if(clustOrder.isEmpty){
              val sKey = keyS.sorted
              if(keyS.min < keyR.min){
                clustOrder += keyS
                clustOrder += keyR
                secOrder += sKey
                secOrder += keyR
                clustHei.update(sLength, height1)
              } else {
                clustOrder += keyR
                clustOrder += keyS
                secOrder += keyR
                secOrder += sKey
                clustHei.update(rLength, height1)
              }
            }
            else {
              val molly = secOrder.exists(_.sameElements(inputKeyPrev))
              val soughtObject = secOrder.filter(_.sameElements(inputKeyPrev))
              val indexOldSplinters = secOrder.indexOf(soughtObject(0))
              println("Index old splinters: " + stringOf(indexOldSplinters))
              println("Remain length: " + stringOf(rLength))
              println("Splinter Length: " + stringOf(sLength))

              if(molly){ // if (molly == true)
                // keyS is not ordered, so sKey used when inserting
                if(keyR.min < keyS.min) {
                  clustOrder.update(indexOldSplinters, keyR)
                  clustOrder.insert(indexOldSplinters + 1, keyS)
                  secOrder.update(indexOldSplinters, keyR)
                  secOrder.insert(indexOldSplinters + 1, sKey)
                }
                else{
                  clustOrder.update(indexOldSplinters, keyS)
                  clustOrder.insert(indexOldSplinters + 1, keyR)
                  secOrder.update(indexOldSplinters, sKey)
                  secOrder.insert(indexOldSplinters + 1, keyR)
                }
              }
              val lengthLeft = (clustOrder.map{ case(i) => i.length}).toArray
              println("Lengthsssss: " + stringOf(lengthLeft))
              val cosmos = (lengthLeft.take(indexOldSplinters+1)).sum
              clustHei.update(cosmos, height1)
              println("COSMOS?!: " + stringOf(cosmos))

              println("Change of the orders: " + stringOf(clustOrder))
              println("Banner: " + stringOf(clustHei))
            }

            //Find the next initial cluster
            largeDiam = largestDiam(keyedMat, remainObj, splinterKeys)
            val inputKey1 = largeDiam.map { case (i, value) => i }
            inputKey = inputKey1.collect

            val keyRemain = inputKeyPrev diff inputKey
            val numKeys = keyRemain.length

            if (numKeys > 1) {
              stack.push(keyRemain)
            }

            keyA = ArrayBuffer()
          }
        }
      }
      found = found + 1
      println(" ")
      println("Found: " + found)
    }
  }
}
