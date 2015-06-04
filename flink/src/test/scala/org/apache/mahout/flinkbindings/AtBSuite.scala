package org.apache.mahout.flinkbindings

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.apache.mahout.math._
import scalabindings._
import RLikeOps._
import org.apache.mahout.math.drm._
import RLikeDrmOps._
import org.apache.mahout.flinkbindings._
import org.apache.mahout.math.function.IntIntFunction
import scala.util.Random
import scala.util.MurmurHash
import scala.util.hashing.MurmurHash3
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.scalatest.Ignore


@RunWith(classOf[JUnitRunner])
class AtBSuite extends FunSuite with DistributedFlinkSuit {
  test("A.t %*% B") {
    val inCoreA = dense((1, 2), (2, 3), (3, 4))
    val inCoreB = dense((1, 2), (3, 4), (11, 4))

    val A = drmParallelize(m = inCoreA, numPartitions = 2)
    val B = drmParallelize(m = inCoreB, numPartitions = 2)

    val res = A.t %*% B

    val expected = inCoreA.t %*% inCoreB
    assert((res.collect - expected).norm < 1e-6)
  }
}