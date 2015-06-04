package org.apache.mahout.flinkbindings.blas

import scala.reflect.ClassTag
import org.apache.mahout.flinkbindings.drm.FlinkDrm
import org.apache.mahout.math.drm.logical.OpAtB
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.mahout.math.Vector
import org.apache.mahout.math.Matrix
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector
import org.apache.mahout.math.drm._
import org.apache.mahout.math.scalabindings._
import RLikeOps._
import org.apache.flink.api.common.functions.GroupReduceFunction
import java.lang.Iterable
import scala.collection.JavaConverters._
import com.google.common.collect.Lists
import org.apache.mahout.flinkbindings.drm.BlockifiedFlinkDrm
import org.apache.mahout.flinkbindings.BlockifiedDrmDataSet
import org.apache.flink.api.scala._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.mahout.flinkbindings.DrmDataSet


object FlinkOpAtB {

  def notZippable[K: ClassTag](op: OpAtB[K], At: FlinkDrm[K], B: FlinkDrm[K]): FlinkDrm[Int] = {
    // TODO: to help Flink's type inference
    // only Int is supported now 
    val rowsAt = At.deblockify.ds.map(new MapFunction[(K, Vector), Tuple2[K, Vector]] {
      def map(tuple: (K, Vector)): Tuple2[K, Vector] = new Tuple2(tuple._1, tuple._2)
    })
    val rowsB = B.deblockify.ds.map(new MapFunction[(K, Vector), Tuple2[K, Vector]] {
      def map(tuple: (K, Vector)): Tuple2[K, Vector] = new Tuple2(tuple._1, tuple._2)
    })
    val joined = rowsAt.join(rowsB).where(0).equalTo(0)

    val ncol = op.ncol
    val nrow = op.nrow
    val blockHeight = 10
    val blockCount = safeToNonNegInt((ncol - 1) / blockHeight + 1)

    val preProduct = joined.flatMap(new FlatMapFunction[Tuple2[Tuple2[K, Vector], Tuple2[K, Vector]], 
                                                        Tuple2[Int, Matrix]] {
      def flatMap(in: Tuple2[Tuple2[K, Vector], Tuple2[K, Vector]],
                  out: Collector[Tuple2[Int, Matrix]]): Unit = {
        val avec = in.f0.f1
        val bvec = in.f1.f1

        0.until(blockCount) map { blockKey =>
          val blockStart = blockKey * blockHeight
          val blockEnd = Math.min(ncol, blockStart + blockHeight)

          // Create block by cross product of proper slice of aRow and qRow
          val outer = avec(blockStart until blockEnd) cross bvec
          out.collect(new Tuple2(blockKey, outer))
        }
      }
    })

    val res: BlockifiedDrmDataSet[Int] = preProduct.groupBy(0).reduceGroup(
            new GroupReduceFunction[Tuple2[Int, Matrix], BlockifiedDrmTuple[Int]] {
      def reduce(values: Iterable[Tuple2[Int, Matrix]], out: Collector[BlockifiedDrmTuple[Int]]): Unit = {
        val it = Lists.newArrayList(values).asScala
//        val (idx, _) = it.head
        val idx = it.head.f0

        val block = it.map(t => t.f1).reduce((m1, m2) => m1 + m2)

        val keys = idx.until(block.nrow).toArray[Int]
        out.collect((keys, block))
      }
    })

    new BlockifiedFlinkDrm(res, ncol)
  }

}

class DrmTupleToFlinkTupleMapper[K: ClassTag] extends MapFunction[(K, Vector), Tuple2[K, Vector]] {
  def map(tuple: (K, Vector)): Tuple2[K, Vector] = tuple match {
    case (key, vec) => new Tuple2[K, Vector](key, vec)
  }
}