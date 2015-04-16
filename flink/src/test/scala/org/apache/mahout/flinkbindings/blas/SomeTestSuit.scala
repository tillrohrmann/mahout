package org.apache.mahout.flinkbindings.blas

import org.scalatest.FunSuite
import org.apache.mahout.math._
import scalabindings._
import RLikeOps._
import drm._
import org.apache.mahout.flinkbindings._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.mahout.math.drm.logical.OpAx
import org.apache.flink.api.common.functions.MapPartitionFunction
import org.apache.flink.util.Collector
import java.lang.Iterable
import org.apache.flink.api.java.ClosureCleaner
import scala.collection.JavaConverters._
import scala.reflect.ClassTag

@RunWith(classOf[JUnitRunner])
class SomeTestSuit extends FunSuite {

  test("Serializable") {
    val ncol = 10
    
    val mapFunction = new MapPartitionFunction[DrmTuple[Int], (Array[Int], Matrix)] {
      def mapPartition(values: Iterable[DrmTuple[Int]], out: Collector[(Array[Int], Matrix)]): Unit = {
        val it = values.asScala.seq

        val (keys, vectors) = it.unzip
        val isDense = vectors.head.isDense

        if (isDense) {
          val matrix = new DenseMatrix(vectors.size, ncol)
          vectors.zipWithIndex.foreach { case (vec, idx) => matrix(idx, ::) := vec }
          out.collect((keys.toArray[Int], matrix))
        } else {
          val matrix = new SparseRowMatrix(vectors.size, ncol, vectors.toArray)
          out.collect((keys.toArray[Int], matrix))
        }
      }
    }

    ClosureCleaner.ensureSerializable(mapFunction)
  }

}