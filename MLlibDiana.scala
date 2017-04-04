package org.apache.spark.mllib.clustering

import java.util.Random

import scala.annotation.tailrec
import scala.collection.mutable

import org.apache.spark.annotation.Since
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.linalg.{BLAS, Vector, Vectors}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
/**
  *
  * @param n the desired number of leaf clusters (default: 4). The actual number could be smaller if
  *          there are no divisible leaf clusters.
  * @param maxIterations the max number of iterations to split clusters (default: 20)
  * @param minDivisibleClusterSize the minimum number of point (if greater than or equal 1.0) or
  *                                the minimum proportion of points (if less than 1.0) of a divisible
  *                                cluster (default: 1)
  * @param seed a random seed (default: has value of the class name)
  */

class Diana private (
                      private var n: Int,
                      private var maxIterations: Int,
                      private var minDivisibleClusterSize: Double,
                      private var seed: Long
                    ) extends Logging {

  import Diana._

  /**
    * Constructs the default configuration
    */

  def this() = this(4, 100, 1.0, classOf[Diana].getName.##)

  /**
    * Sets the desired number of leaf clusters (default: 4).
    * The actual number could be smaller if there are no divisible leaf clusters.
    */

  def setN(n: Int): this.type ={
    require(n > 0, s"N must be positive but got $n.")
    this.n = n
    this
  }

  /**
    * Gets the desired number of leaf clusters
    */

  def getN: Int = this.n

  /**
    * Sets the max number of Diana Method iterations to split clusters (default: 100)
    */

  def setMaxIterations(maxIterations: Int): this.type ={
    require(maxIterations > 0, s"maxIterations must be positive but got $maxIterations.")
    this.maxIterations = maxIterations
    this
  }

  /**
    * Gets the max number of Diana iterations to split clusters
    */

  def getMaxIterations: Int = this.maxIterations

  /**
    *Sets the minimum number of points(if greater than or equal to "1.0") or the minimum
    * proportion of points(if less than "1.0") of a divisible cluster (default: 1)
    */

  def setMinDivisibleClusterSize(minDivisibleClusterSize: Double): this.type = {
    require(minDivisibleClusterSize > 0.0,
      s"minDivisibleClusterSize must be positive but got $minDivisibleClusterSize.")
    this.minDivisibleClusterSize = minDivisibleClusterSize
    this
  }

  /**
    * Gets the minimum number of points(if greater than or equal to "1.0") or the minimum
    * proportion of points(if less than "1.0") of a divisible cluster (default: 1)
    */

  def getMinDivisibleClusterSize: Double = minDivisibleClusterSize

  /**
    * Sets the random seed(default: hash value of the class name)
    */

  def setSeed(seed: Long): this.type = {
    this.seed = seed
    this
  }

  /**
    * Gets the random seed(default: hash value of the class name)
    */

  def getSeed: Long = this.seed

  /**
    *
    * @param input RDD of vectors
    * @return model for the DIANA
    */
  def run(input: RDD[Vector]): DianaModel = {
    if (input.getStorageLevel == StorageLevel.NONE) {
      logWarning(s"The input RDD ${input.id} is not directly cached, which may hurt performance if"
        + " its parent RDDs are also not cached.")
    }
    val d = input.map(_.size).first()
    logInfo(s"Feature dimension: $d.")
    // Compute and cache vector norms for fast distance computation.
    val norms = input.map(v => Vectors.norm(v, 2.0)).persist(StorageLevel.MEMORY_AND_DISK)
    val vectors = input.zip(norms).map { case (x, norm) => new VectorWithNorm(x, norm) }
    var assignments = vectors.map(v => (ROOT_INDEX, v))
    var activeClusters = summarize(d, assignments)
    val rootSummary = activeClusters(ROOT_INDEX)
    val p = rootSummary.size
    logInfo(s"Number of points: $p.")
    logInfo(s"Initial cost: ${rootSummary.cost}.")
    val minSize = if (minDivisibleClusterSize >= 1.0) {
      math.ceil(minDivisibleClusterSize).toLong
    } else {
      math.ceil(minDivisibleClusterSize * n).toLong
    }
    logInfo(s"The minimum number of points of a divisible cluster is $minSize.")
    var inactiveClusters = mutable.Seq.empty[(Long, ClusterSummary)]
    val random = new Random(seed)
    var numLeafClustersNeeded = n - 1
    var level = 1
    var preIndices: RDD[Long] = null
    var indices: RDD[Long] = null
    while (activeClusters.nonEmpty && numLeafClustersNeeded > 0 && level < LEVEL_LIMIT) {
      // Divisible clusters are sufficiently large and have non-trivial cost.
      var divisibleClusters = activeClusters.filter { case (_, summary) =>
        (summary.size >= minSize) && (summary.cost > MLUtils.EPSILON * summary.size)
      }
      // If we don't need all divisible clusters, take the larger ones.
      if (divisibleClusters.size > numLeafClustersNeeded) {
        divisibleClusters = divisibleClusters.toSeq.sortBy { case (_, summary) =>
          -summary.size
        }.take(numLeafClustersNeeded)
          .toMap
      }
      if (divisibleClusters.nonEmpty) {
        val divisibleIndices = divisibleClusters.keys.toSet
        logInfo(s"Dividing ${divisibleIndices.size} clusters on level $level.")
        var newClusterCenters = divisibleClusters.flatMap { case (index, summary) =>
          val (left, right) = splitCenter(summary.center, random)
          Iterator((leftChildIndex(index), left), (rightChildIndex(index), right))
        }.map(identity) // workaround for a Scala bug (SI-7005) that produces a not serializable map
        var newClusters: Map[Long, ClusterSummary] = null
        var newAssignments: RDD[(Long, VectorWithNorm)] = null
        for (iter <- 0 until maxIterations) {
          newAssignments = updateAssignments(assignments, divisibleIndices, newClusterCenters)
            .filter { case (index, _) =>
              divisibleIndices.contains(parentIndex(index))
            }
          newClusters = summarize(d, newAssignments)
          newClusterCenters = newClusters.mapValues(_.center).map(identity)
        }
        if (preIndices != null) preIndices.unpersist()
        preIndices = indices
        indices = updateAssignments(assignments, divisibleIndices, newClusterCenters).keys
          .persist(StorageLevel.MEMORY_AND_DISK)
        assignments = indices.zip(vectors)
        inactiveClusters ++= activeClusters
        activeClusters = newClusters
        numLeafClustersNeeded -= divisibleClusters.size
      } else {
        logInfo(s"None active and divisible clusters left on level $level. Stop iterations.")
        inactiveClusters ++= activeClusters
        activeClusters = Map.empty
      }
      level += 1
    }
    if(indices != null) indices.unpersist()
    val clusters = activeClusters ++ inactiveClusters
    val root = buildTree(clusters)
    new DianaModel(root)
  }

  /**
    * Java-friendly version of 'run()'
    */

  def run(data: JavaRDD[Vector]): DianaModel = run(data.rdd)
}

private object Diana extends Serializable {

  /**
    * The index of the root node of a tree
    */

  private val ROOT_INDEX: Long = 1

  private val MAX_DIVISIBLE_CLUSTER_INDEX: Long = Long.MaxValue/2

  private val LEVEL_LIMIT = math.log10(Long.MaxValue)/math.log10(2)

  /**
    * Returns the left child index of the given node index
    */

  private def leftChildIndex(index: Long):Long ={
    require(index <= MAX_DIVISIBLE_CLUSTER_INDEX, s"Child index out of bound: 2 * $index.")
    2 * index
  }

  /**
    * Returns the right child index of the given node index
    */

  private def rightChildIndex(index: Long):Long = {
    require(index <= MAX_DIVISIBLE_CLUSTER_INDEX, s"Child index out of bound: 2 * $index + 1.")
    2 * index + 1
  }

  /**
    * Returns the parent index of the given node index, or 0 if the input is 1 (root).
    */
  private def parentIndex(index: Long):Long = {
    index / 2
  }

  /**
    * Summarize data by each cluster as Map
    * @param d feature dimension
    * @param assignments pairs of point and its cluster index
    * @return a map from cluster indices to corresponding cluster summaries
    */


  private def summarize(
                         d: Int,
                         assignments: RDD[(Long, VectorWithNorm)]): Map[Long, ClusterSummary] = {
    assignments.aggregateByKey(new ClusterSummaryAggregator(d))(
      seqOp = (agg, v) => agg.add(v),
      combOp = (agg1, agg2) => agg1.merge(agg2)
    ).mapValues(_.summary)
      .collect().toMap
  }

  /**
    * Cluster summary aggregator
    * @param d feature dimension
    */

  private class ClusterSummaryAggregator(val d: Int) extends Serializable {
    private var p: Long = 0L
    private val sum: Vector = Vectors.zeros(d)
    private var sumSq: Double = 0.0

    /** Adds a point */
    def add(v: VectorWithNorm): this.type ={
      p += 1L
      //TODO: use a numerically stable approach to estimate cost
      sumSq += v.norm * v.norm
      BLAS.axpy(1.0, v.vector, sum)
      this
    }

    /** Merges another aggregator */
    def merge(other: ClusterSummaryAggregator):this.type ={
      p += other.p
      sumSq += other.sumSq
      BLAS.axpy(1.0, other.sum, sum)
      this
    }

    /** Returns a summary */
    def summary: ClusterSummary ={
      val mean = sum.copy
      if(p > 0L){
        BLAS.scal(1.0/p, mean)
      }
      val center = new VectorWithNorm(mean)
      val cost = math.max(sumSq - p* center.norm * center.norm, 0.0)
      new ClusterSummary(p, center, cost)
    }
  }

  /**
    * Bisects a cluster center
    *
    * @param center current cluster center
    * @param random a random number generator
    * @return initial clusters
    */

  private def splitCenter(
                         center: VectorWithNorm,
                         random: Random
                         ): (VectorWithNorm, VectorWithNorm) ={
    val d = center.vector.size
    val norm = center.norm
    val level = 1e-4 * norm
    val noise = Vectors.dense(Array.fill(d)(random.nextDouble()))
    val left = center.vector.copy
    BLAS.axpy(-level, noise, left)
    val right = center.vector.copy
    BLAS.axpy(level, noise, right)
    (new VectorWithNorm(left), new VectorWithNorm(right))
  }

  /**
    * Updates assignments
    *
    * @param assignments current assignments
    * @param divisibleIndices divisible cluster indices
    * @param newClusterCenters new cluster centers
    * @return new assignments
    */

  private def updateAssignments(
                                 assignments: RDD[(Long, VectorWithNorm)],
                                 divisibleIndices: Set[Long],
                                 newClusterCenters: Map[Long, VectorWithNorm]): RDD[(Long, VectorWithNorm)] = {
    assignments.map { case (index, v) =>
      if (divisibleIndices.contains(index)) {
        val children = Seq(leftChildIndex(index), rightChildIndex(index))
        val newClusterChildren = children.filter(newClusterCenters.contains(_))
        if (newClusterChildren.nonEmpty) {
          val selected = newClusterChildren.minBy { child =>
            //TODO change the kmeans
            KMeans.fastSquaredDistance(newClusterCenters(child), v)
          }
          (selected, v)
        } else {
          (index, v)
        }
      } else {
        (index, v)
      }
    }
  }

  /**
    * Builds a clustering tree by re-indexing internal and leaf clusters
    * @param clusters a map from cluster indices to corresponding cluster summaries
    * @return the root node of the clustering tree
    */

  private def buildTree(clusters: Map[Long, ClusterSummary]): ClusteringTreeNode = {
    var leafIndex = 0
    var internalIndex = -1

    /**
      * Builds a subtree from this given node index.
      */
    def buildSubTree(rawIndex: Long): ClusteringTreeNode = {
      val cluster = clusters(rawIndex)
      val size = cluster.size
      val center = cluster.center
      val cost = cluster.cost
      val isInternal = clusters.contains(leftChildIndex(rawIndex))
      if (isInternal) {
        val index = internalIndex
        internalIndex -= 1
        val leftIndex = leftChildIndex(rawIndex)
        val rightIndex = rightChildIndex(rawIndex)
        val indexes = Seq(leftIndex, rightIndex).filter(clusters.contains(_))
        val height = math.sqrt(indexes.map { childIndex =>
          //TODO change the kmeans
          KMeans.fastSquaredDistance(center, clusters(childIndex).center)
        }.max)
        val children = indexes.map(buildSubTree(_)).toArray
        new ClusteringTreeNode(index, size, center, cost, height, children)
      } else {
        val index = leafIndex
        leafIndex += 1
        val height = 0.0
        new ClusteringTreeNode(index, size, center, cost, height, Array.empty)
      }
    }

    buildSubTree(ROOT_INDEX)
  }

  /**
    * Summary of a cluster.
    *
    * @param size the number of points within this cluster
    * @param center the center of the points within this cluster
    * @param cost the sum of squared distances to the center
    */
  private case class ClusterSummary(size: Long, center: VectorWithNorm, cost: Double)
}

/**
  * Represents a node in a clustering tree
  *
  * @param index node index, negative for internal nodes and non- negative for leaf node
  * @param size size of a cluster
  * @param centerWithNorm cluster center with norm
  * @param cost cost of the cluster, ie the sum of squared distances to the center
  * @param height height of the node in the dendrogram. Currently this is defined as the max distance
  *               from the center to the centers of the children's but subject to change
  * @param children children node
  */

private[clustering] class ClusteringTreeNode private[clustering] (
                                                                   val index: Int,
                                                                   val size: Long,
                                                                   private[clustering] val centerWithNorm: VectorWithNorm,
                                                                   val cost: Double,
                                                                   val height: Double,
                                                                   val children: Array[ClusteringTreeNode]) extends Serializable {

  /** Whether this is a leaf node. */
  val isLeaf: Boolean = children.isEmpty

  require((isLeaf && index >= 0) || (!isLeaf && index < 0))

  /** Cluster center. */
  def center: Vector = centerWithNorm.vector

  /** Predicts the leaf cluster node index that the input point belongs to. */
  def predict(point: Vector): Int = {
    val (index, _) = predict(new VectorWithNorm(point))
    index
  }

  /** Returns the full prediction path from root to leaf. */
  def predictPath(point: Vector): Array[ClusteringTreeNode] = {
    predictPath(new VectorWithNorm(point)).toArray
  }

  /** Returns the full prediction path from root to leaf. */
  private def predictPath(pointWithNorm: VectorWithNorm): List[ClusteringTreeNode] = {
    if (isLeaf) {
      this :: Nil
    } else {
      val selected = children.minBy { child =>
        //TODO replace kmeans
        KMeans.fastSquaredDistance(child.centerWithNorm, pointWithNorm)
      }
      selected :: selected.predictPath(pointWithNorm)
    }
  }

  /**
    * Computes the cost (squared distance to the predicted leaf cluster center) of the input point.
    */
  def computeCost(point: Vector): Double = {
    val (_, cost) = predict(new VectorWithNorm(point))
    cost
  }

  /**
    * Predicts the cluster index and the cost of the input point.
    */
  private def predict(pointWithNorm: VectorWithNorm): (Int, Double) = {
    //TODO replace kmeans
    predict(pointWithNorm, KMeans.fastSquaredDistance(centerWithNorm, pointWithNorm))
  }

  /**
    * Predicts the cluster index and the cost of the input point.
    * @param pointWithNorm input point
    * @param cost the cost to the current center
    * @return (predicted leaf cluster index, cost)
    */
  @tailrec
  private def predict(pointWithNorm: VectorWithNorm, cost: Double): (Int, Double) = {
    if (isLeaf) {
      (index, cost)
    } else {
      val (selectedChild, minCost) = children.map { child =>
        //TODO replace kmeans
        (child, KMeans.fastSquaredDistance(child.centerWithNorm, pointWithNorm))
      }.minBy(_._2)
      selectedChild.predict(pointWithNorm, minCost)
    }
  }

  /**
    * Returns all leaf nodes from this node.
    */
  def leafNodes: Array[ClusteringTreeNode] = {
    if (isLeaf) {
      Array(this)
    } else {
      children.flatMap(_.leafNodes)
    }
  }
}


