package org.apache.mahout.flinkbindings.blas

import scala.reflect.ClassTag
import org.apache.mahout.flinkbindings.drm.FlinkDrm
import org.apache.mahout.flinkbindings._
import org.apache.mahout.math.drm.drmBroadcast
import org.apache.mahout.math.drm.logical.OpAx
import org.apache.mahout.math.Matrix
import org.apache.flink.api.common.functions.MapFunction
import org.apache.mahout.flinkbindings.drm.BlockifiedFlinkDrm

import org.apache.mahout.math._
import scalabindings._
import RLikeOps._

object FlinkOpAx {

  def blockifiedBroadcastAx[K: ClassTag](op: OpAx[K], A: FlinkDrm[K]): FlinkDrm[K] = {
    implicit val ctx = A.context
    val x = drmBroadcast(op.x)

    val out = A.blockify.ds.map(new MapFunction[(Array[K], Matrix), (Array[K], Matrix)] {
      def map(tuple: (Array[K], Matrix)): (Array[K], Matrix) = tuple match {
        case (keys, mat) => (keys, (mat %*% x).toColMatrix)
      }
    })

    new BlockifiedFlinkDrm(out, op.nrow.toInt)
  }
}