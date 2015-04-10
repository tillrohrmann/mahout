package org.apache.mahout.flinkbindings.drm

import scala.reflect.ClassTag
import org.apache.mahout.math.drm.CheckpointedDrm
import org.apache.mahout.math.Matrix

class CheckpointedFlinkDrm[K: ClassTag](val drm: CheckpointedDrm[K]) extends CheckpointedDrm[K] with FlinkDrm[K] {

  def collect: Matrix = {
    null
  }

  def dfsWrite(path: String) = {

  }

  def newRowCardinality(n: Int): CheckpointedDrm[K] = {
    null
  }

}