package org.apache.spark.mllib.clustering

import org.json4s._
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Since
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

class DianaModel private[clustering] (
                                                 private[clustering] val root: ClusteringTreeNode
                                               ) extends Serializable with Saveable with Logging {

  /**
    * Leaf cluster centers.
    */
  def clusterCenters: Array[Vector] = root.leafNodes.map(_.center)

  /**
    * Number of leaf clusters.
    */
  lazy val n: Int = clusterCenters.length

  /**
    * Predicts the index of the cluster that the input point belongs to.
    */
  def predict(point: Vector): Int = {
    root.predict(point)
  }

  /**
    * Predicts the indices of the clusters that the input points belong to.
    */
  def predict(points: RDD[Vector]): RDD[Int] = {
    points.map { p => root.predict(p) }
  }

  /**
    * Java-friendly version of `predict()`.
    */
  def predict(points: JavaRDD[Vector]): JavaRDD[java.lang.Integer] =
    predict(points.rdd).toJavaRDD().asInstanceOf[JavaRDD[java.lang.Integer]]

  /**
    * Computes the squared distance between the input point and the cluster center it belongs to.
    */
  def computeCost(point: Vector): Double = {
    root.computeCost(point)
  }

  /**
    * Computes the sum of squared distances between the input points and their corresponding cluster
    * centers.
    */
  def computeCost(data: RDD[Vector]): Double = {
    data.map(root.computeCost).sum()
  }

  /**
    * Java-friendly version of `computeCost()`.
    */
  def computeCost(data: JavaRDD[Vector]): Double = this.computeCost(data.rdd)

  override def save(sc: SparkContext, path: String): Unit = {
    DianaModel.SaveLoad.save(sc, this, path)
  }

  override protected def formatVersion: String = "1.0"
}

object DianaModel extends Loader[DianaModel] {

  override def load(sc: SparkContext, path: String): DianaModel = {
    val (loadedClassName, formatVersion, metadata) = Loader.loadMetadata(sc, path)
    implicit val formats = DefaultFormats
    val rootId = (metadata \ "rootId").extract[Int]
    val className = SaveLoad.thisClassName
    (loadedClassName, formatVersion) match {
      case (className, "1.0") =>
        val model = SaveLoad.load(sc, path, rootId)
        model
      case _ => throw new Exception(
        s"DianaModel.load did not recognize model with (className, format version):" +
          s"($loadedClassName, $formatVersion).  Supported:\n" +
          s"  ($className, 1.0)")
    }
  }

  private case class Data(index: Int, size: Long, center: Vector, norm: Double, cost: Double,
                          height: Double, children: Seq[Int])

  private object Data {
    def apply(r: Row): Data = Data(r.getInt(0), r.getLong(1), r.getAs[Vector](2), r.getDouble(3),
      r.getDouble(4), r.getDouble(5), r.getSeq[Int](6))
  }

  private[clustering] object SaveLoad {
    private val thisFormatVersion = "1.0"

    private[clustering]
    val thisClassName = "org.apache.spark.mllib.clustering.DianaModel"

    def save(sc: SparkContext, model: DianaModel, path: String): Unit = {
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()
      val metadata = compact(render(
        ("class" -> thisClassName) ~ ("version" -> thisFormatVersion)
          ~ ("rootId" -> model.root.index)))
      sc.parallelize(Seq(metadata), 1).saveAsTextFile(Loader.metadataPath(path))

      val data = getNodes(model.root).map(node => Data(node.index, node.size,
        node.centerWithNorm.vector, node.centerWithNorm.norm, node.cost, node.height,
        node.children.map(_.index)))
      spark.createDataFrame(data).write.parquet(Loader.dataPath(path))
    }

    private def getNodes(node: ClusteringTreeNode): Array[ClusteringTreeNode] = {
      if (node.children.isEmpty) {
        Array(node)
      } else {
        node.children.flatMap(getNodes(_)) ++ Array(node)
      }
    }

    def load(sc: SparkContext, path: String, rootId: Int): DianaModel = {
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()
      val rows = spark.read.parquet(Loader.dataPath(path))
      Loader.checkSchema[Data](rows.schema)
      val data = rows.select("index", "size", "center", "norm", "cost", "height", "children")
      val nodes = data.rdd.map(Data.apply).collect().map(d => (d.index, d)).toMap
      val rootNode = buildTree(rootId, nodes)
      new DianaModel(rootNode)
    }

    private def buildTree(rootId: Int, nodes: Map[Int, Data]): ClusteringTreeNode = {
      val root = nodes.get(rootId).get
      if (root.children.isEmpty) {
        new ClusteringTreeNode(root.index, root.size, new VectorWithNorm(root.center, root.norm),
          root.cost, root.height, new Array[ClusteringTreeNode](0))
      } else {
        val children = root.children.map(c => buildTree(c, nodes))
        new ClusteringTreeNode(root.index, root.size, new VectorWithNorm(root.center, root.norm),
          root.cost, root.height, children.toArray)
      }
    }
  }
}