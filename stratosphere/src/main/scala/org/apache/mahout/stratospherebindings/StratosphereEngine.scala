/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.mahout.stratospherebindings

import org.apache.mahout.math.scalabindings._
import RLikeOps._
import org.apache.mahout.math.drm._
import org.apache.mahout.math.drm.logical._
import org.apache.mahout.stratospherebindings.blas.AewB
import org.apache.mahout.stratospherebindings.drm.CheckpointedDrmStratosphere
import scala.reflect.ClassTag
import org.apache.mahout.math.{VectorWritable, RandomAccessSparseVector, Vector, Matrix}
import collection.JavaConversions._

class StratosphereEngine(val context: StratosphereDistributedContext) extends DistributedEngine {

  /** Second optimizer pass. Translate previously rewritten logical pipeline into physical engine plan. */
  override def toPhysical[K: ClassTag](plan: DrmLike[K], ch: CacheHint.CacheHint): CheckpointedDrm[K] = {
    val drmDS = translate(plan)

    new CheckpointedDrmStratosphere[K](drmDS, context)
  }

  /** Engine-specific colSums implementation based on a checkpoint. */
  override def colSums[K:ClassTag](drm:CheckpointedDrm[K]):Vector = {
    null
  }

  /** Engine-specific colMeans implementation based on a checkpoint. */
  override def colMeans[K:ClassTag](drm:CheckpointedDrm[K]):Vector = {
    null
  }

  /** Engine-specific numNonZeroElementsPerColumn implementation based on a checkpoint. */
  override def numNonZeroElementsPerColumn[K: ClassTag](drm: CheckpointedDrm[K]): Vector = {
    null
  }

  override def norm[K: ClassTag](drm: CheckpointedDrm[K]): Double = {
    -1
  }

  /** Broadcast support */
  override def drmBroadcast(v: Vector)(implicit ctx: DistributedContext): BCast[Vector] = {
    null
  }

  /** Broadcast support */
  override def drmBroadcast(m: Matrix)(implicit ctx: DistributedContext): BCast[Matrix] = {
    null
  }

  /** Load DRM from hdfs (as in Mahout DRM format) */
  override def drmFromHDFS (path: String)(implicit ctx: DistributedContext): CheckpointedDrm[_] = {
    null
  }

  /** Parallelize in-core matrix as spark distributed matrix, using row ordinal indices as data set keys. */
  override def drmParallelizeWithRowIndices(m: Matrix, numPartitions: Int = 1)
                                  (implicit ctx: DistributedContext): CheckpointedDrm[Int] = {

    val rowCollection = (0 until m.numRows).map{r => JT(r,m(r, ::):Vector)}
    val ds = ctx.fromCollection(rowCollection).setParallelism(numPartitions)
    
    new CheckpointedDrmStratosphere[Int](ds, ctx)
  }

  /** Parallelize in-core matrix as spark distributed matrix, using row labels as a data set keys. */
  override def drmParallelizeWithRowLabels(m: Matrix, numPartitions: Int = 1)
                                 (implicit ctx: DistributedContext): CheckpointedDrm[String] = {
    val bindings = m.getRowLabelBindings

    val rowCollection = for((label, idx) <- bindings) yield JT(label, m(idx, ::):Vector)
    val ds = ctx.fromCollection(rowCollection).setParallelism(numPartitions)

    new CheckpointedDrmStratosphere[String](ds, ctx)
  }

  /** This creates an empty DRM with specified number of partitions and cardinality. */
  override def drmParallelizeEmpty(nrow: Int, ncol: Int, numPartitions: Int = 10)
                         (implicit ctx: DistributedContext): CheckpointedDrm[Int] = {

    val rowCollection = (0 until nrow).map{row => JT(row, new RandomAccessSparseVector(ncol):Vector)}
    val ds = ctx.fromCollection(rowCollection).setParallelism(numPartitions)

    new CheckpointedDrmStratosphere[Int](ds, ctx)
  }

  /** Creates empty DRM with non-trivial height */
  override def drmParallelizeEmptyLong(nrow: Long, ncol: Int, numPartitions: Int = 10)
                             (implicit ctx: DistributedContext): CheckpointedDrm[Long] = {
    val rowCollection = (0L until nrow).map{row => JT(row, new RandomAccessSparseVector(ncol):Vector)}
    val ds = ctx.fromCollection(rowCollection).setParallelism(numPartitions)

    new CheckpointedDrmStratosphere[Long](ds, ctx)
  }

  private def translate[K:ClassTag](plan: DrmLike[K]): DrmDS[K] = {
    plan match {
      case op@OpAewScalar(a, s, "+") => AewB.a_plus_scalar(translate(a)(op.classTagA), s)
      case op@OpAewScalar(a, s, "-") => AewB.a_minus_scalar(translate(a)(op.classTagA),s)
      case op@OpAewScalar(a, s, "-:") => AewB.scalar_minus_a(s, translate(a)(op.classTagA))
      case op@OpAewScalar(a, s, "*") => AewB.a_times_scalar(translate(a)(op.classTagA), s)
      case op@OpAewScalar(a, s, "/") => AewB.a_div_scalar(translate(a)(op.classTagA), s)
      case op@OpAewScalar(a, s, "/:") => AewB.scalar_div_a(s, translate(a)(op.classTagA))
      case op@OpAewB(a, b, "+") => AewB.a_plus_b(translate(a)(op.classTagA), translate(b)(op.classTagB))
      case op@OpAewB(a, b, "-") => AewB.a_minus_b(translate(a)(op.classTagA), translate(b)(op.classTagB))
      case op@OpAewB(a, b, "*") => AewB.a_hadamard_b(translate(a)(op.classTagA), translate(b)(op.classTagB))
      case op@OpAewB(a, b, "/") => AewB.a_eldiv_b(translate(a)(op.classTagA), translate(b)(op.classTagB))
      case op => throw new IllegalArgumentException(s"StratosphereEngine does not support operation $op.")
    }
  }
}
