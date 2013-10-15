package org.apache.spark.graph

import org.scalatest.FunSuite

import org.apache.spark.SparkContext
import org.apache.spark.graph.LocalSparkContext._


class GraphSuite extends FunSuite with LocalSparkContext {

//  val sc = new SparkContext("local[4]", "test")

  test("Graph Creation") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val rawEdges = (0L to 100L).zip((1L to 99L) :+ 0L)
      val edges = sc.parallelize(rawEdges)
      val graph = Graph(edges)
      assert( graph.edges.count() === rawEdges.size )
    }
  }

  test("aggregateNeighbors") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val n = 10000
      val adj: Seq[(Vid, Vid)] = (1 to n).map(x => (0: Vid, x: Vid))
      val star = Graph(sc.parallelize(adj))

      val indegrees = star.aggregateNeighbors(
        (vid, edge) => Some(1),
        (a: Int, b: Int) => a + b,
        EdgeDirection.In).vertices.map(v => (v.id, v.data._2.getOrElse(0)))
      assert(indegrees.collect().toSet === Set((0, 0)) ++ (1 to n).map(x => (x, 1)).toSet)

      val outdegrees = star.aggregateNeighbors(
        (vid, edge) => Some(1),
        (a: Int, b: Int) => a + b,
        EdgeDirection.Out).vertices.map(v => (v.id, v.data._2.getOrElse(0)))
      assert(outdegrees.collect().toSet === (1 to n).map(x => (x, 0)).toSet ++ Set((0, n)))

      val noVertexValues = star.aggregateNeighbors[Int](
        (vid: Vid, edge: EdgeTriplet[Int, Int]) => None,
        (a: Int, b: Int) => throw new Exception("reduceFunc called unexpectedly"),
        EdgeDirection.In).vertices.map(v => (v.id, v.data._2))
      assert(noVertexValues.collect().toSet === (0 to n).map(x => (x, None)).toSet)
    }
  }

  test("aggregateNeighbors example") {
    withSpark(new SparkContext("local", "test")) { sc =>
      import org.apache.spark.graph._

      val graph = Graph(sc.parallelize(List((0, 1), (0, 2), (2, 1)))).mapVertices(v => v.id.toInt)

      def mapFunc(vid: Vid, edge: EdgeTriplet[Int, Int]): Option[(Int, Double)] =
        Some((edge.otherVertex(vid).data, 1))

      def mergeFunc(a: (Int, Double), b: (Int, Double)): (Int, Double) =
        (a._1 + b._1, a._2 + b._2)

      val agg = graph.aggregateNeighbors[(Int,Double)](mapFunc _, mergeFunc _, EdgeDirection.In)

      val averageFollowerAge: Graph[Double, Int] =
        agg.mapVertices { v => v.data._2 match {
          case Some((sum, followers)) => sum.toDouble / followers
          case None => Double.NaN
        }}

      averageFollowerAge.vertices.collect
    }
  }

 /* test("joinVertices") {
    sc = new SparkContext("local", "test")
    val vertices = sc.parallelize(Seq(Vertex(1, "one"), Vertex(2, "two"), Vertex(3, "three")), 2)
    val edges = sc.parallelize((Seq(Edge(1, 2, "onetwo"))))
    val g: Graph[String, String] = new GraphImpl(vertices, edges)

    val tbl = sc.parallelize(Seq((1, 10), (2, 20)))
    val g1 = g.joinVertices(tbl, (v: Vertex[String], u: Int) => v.data + u)

    val v = g1.vertices.collect().sortBy(_.id)
    assert(v(0).data === "one10")
    assert(v(1).data === "two20")
    assert(v(2).data === "three")

    val e = g1.edges.collect()
    assert(e(0).data === "onetwo")
  }
  */

//  test("graph partitioner") {
//    sc = new SparkContext("local", "test")
//    val vertices = sc.parallelize(Seq(Vertex(1, "one"), Vertex(2, "two")))
//    val edges = sc.parallelize(Seq(Edge(1, 2, "onlyedge")))
//    var g = Graph(vertices, edges)
//
//    g = g.withPartitioner(4, 7)
//    assert(g.numVertexPartitions === 4)
//    assert(g.numEdgePartitions === 7)
//
//    g = g.withVertexPartitioner(5)
//    assert(g.numVertexPartitions === 5)
//
//    g = g.withEdgePartitioner(8)
//    assert(g.numEdgePartitions === 8)
//
//    g = g.mapVertices(x => x)
//    assert(g.numVertexPartitions === 5)
//    assert(g.numEdgePartitions === 8)
//
//    g = g.mapEdges(x => x)
//    assert(g.numVertexPartitions === 5)
//    assert(g.numEdgePartitions === 8)
//
//    val updates = sc.parallelize(Seq((1, " more")))
//    g = g.updateVertices(
//      updates,
//      (v, u: Option[String]) => if (u.isDefined) v.data + u.get else v.data)
//    assert(g.numVertexPartitions === 5)
//    assert(g.numEdgePartitions === 8)
//
//    g = g.reverse
//    assert(g.numVertexPartitions === 5)
//    assert(g.numEdgePartitions === 8)
//
//  }
}
