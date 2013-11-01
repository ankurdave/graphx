package org.apache.spark.bagel

import org.apache.spark._
import org.apache.spark.SparkContext._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo

object BagelPageRank {
  def main(args: Array[String]) = {
    val host = args(0)
    val taskType = args(1)
    val fname = args(2)
    val options =  args.drop(3).map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => (opt -> v)
        case _ => throw new IllegalArgumentException("Invalid argument: " + arg)
      }
    }

    val serializer = "org.apache.spark.serializer.KryoSerializer"
    System.setProperty("spark.serializer", serializer)
    System.setProperty("spark.kryo.registrator", classOf[PRKryoRegistrator].getName)

    var numIter = Int.MaxValue

    options.foreach{
      case ("numIter", v) => numIter = v.toInt
      case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
    }
    
    val sc = new SparkContext(host, "PageRank(" + fname + ")")
    val input = sc.textFile(fname)
    var rawVerts = input.flatMap(line => {
      if (line.startsWith("#")) {
        None
      } else {
        val fields = line.split("\\s+")
        val (src, dst) = (fields(0).toInt, fields(1).toInt)
        Some((src, dst))
      }
    }).groupByKey(sc.defaultParallelism)
    val n = rawVerts.count()
    val verts = rawVerts.map { case (src, dsts) => (src, new PRVertex(1.0 / n, dsts.toArray))}.cache()

    val messages = sc.parallelize(Array[(Int, PRMessage)]())
    val result = Bagel.run[Int, PRVertex, PRMessage, Double](
      sc, verts, messages, new PRCombiner(), sc.defaultParallelism)(computeWithCombiner(n, numIter))
    println("Total rank: " + result.map { case (id, v) => v.value }.reduce(_ + _))
    System.exit(0)
  }

  def computeWithCombiner(numVertices: Long, numIter: Int)(
    self: PRVertex, messageSum: Option[Double], superstep: Int
  ): (PRVertex, Array[PRMessage]) = {
    val newValue = messageSum match {
      case Some(msgSum) if msgSum != 0 =>
        0.15 / numVertices + 0.85 * msgSum
      case _ => self.value
    }

    val terminate = superstep >= numIter

    val outbox: Array[PRMessage] =
      if (!terminate)
        self.outEdges.map(targetId =>
          new PRMessage(targetId, newValue / self.outEdges.size))
      else
        Array[PRMessage]()

    (new PRVertex(newValue, self.outEdges, !terminate), outbox)
  }
}


class PRCombiner extends Combiner[PRMessage, Double] with Serializable {
  def createCombiner(msg: PRMessage): Double =
    msg.value
  def mergeMsg(combiner: Double, msg: PRMessage): Double =
    combiner + msg.value
  def mergeCombiners(a: Double, b: Double): Double =
    a + b
}

class PRVertex() extends Vertex with Serializable {
  var value: Double = _
  var outEdges: Array[Int] = _
  var active: Boolean = _

  def this(value: Double, outEdges: Array[Int], active: Boolean = true) {
    this()
    this.value = value
    this.outEdges = outEdges
    this.active = active
  }
}

class PRMessage() extends Message[Int] with Serializable {
  var targetId: Int = _
  var value: Double = _

  def this(targetId: Int, value: Double) {
    this()
    this.targetId = targetId
    this.value = value
  }
}

class PRKryoRegistrator extends KryoRegistrator {
  def registerClasses(kryo: Kryo) {
    kryo.register(classOf[PRVertex])
    kryo.register(classOf[PRMessage])
  }
}
