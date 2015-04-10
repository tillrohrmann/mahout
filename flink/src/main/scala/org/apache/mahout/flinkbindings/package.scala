package org.apache.mahout

import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.mahout.flinkbindings.FlinkDistributedContext
import org.apache.mahout.flinkbindings.drm.BlockifiedFlinkDrm
import org.apache.mahout.flinkbindings.drm.RowsFlinkDrm
import org.apache.mahout.math.drm._
import org.slf4j.LoggerFactory
import org.apache.mahout.flinkbindings.drm.CheckpointedFlinkDrm
import scala.reflect.ClassTag
import org.apache.mahout.flinkbindings.drm.FlinkDrm

package object flinkbindings {

  private[flinkbindings] val log = LoggerFactory.getLogger("apache.org.mahout.flinkbingings")

  /** Row-wise organized DRM dataset type */
  type DrmDataSet[K] = DataSet[DrmTuple[K]]

  /**
   * Blockifed DRM dataset (keys of original DRM are grouped into array corresponding to rows of Matrix
   * object value
   */
  type BlockifiedDrmDataSet[K] = DataSet[BlockifiedDrmTuple[K]]

  implicit def wrapContext(env: ExecutionEnvironment): FlinkDistributedContext =
    new FlinkDistributedContext(env)
  implicit def unwrapContext(ctx: FlinkDistributedContext): ExecutionEnvironment = ctx.env

  implicit def wrapDrm[K: ClassTag](drm: CheckpointedDrm[K]): FlinkDrm[K] = new CheckpointedFlinkDrm(drm)

}