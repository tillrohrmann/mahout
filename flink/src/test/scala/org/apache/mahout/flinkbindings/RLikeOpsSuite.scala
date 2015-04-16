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

@RunWith(classOf[JUnitRunner])
class RLikeOpsSuite extends FunSuite with DistributedFlinkSuit {

  //  test("A %*% x") {
  //    val inCoreA = dense((1, 2, 3), (2, 3, 4), (3, 4, 5))
  //    val A = drmParallelize(m = inCoreA, numPartitions = 2)
  //    val x: Vector = (0, 1, 2)
  //
  //    val res = A %*% x
  //
  //    val b = res.collect(::, 0)
  //    assert(b == dvec(8, 11, 14))
  //  }

  test("Power interation") {
    val SEED = 0xFF0F
    val d = 100
    val inCoreA = Matrices.gaussianView(d, d, SEED)
    val A = drmParallelize(m = inCoreA, numPartitions = 2)

    var x: Vector = 1 to d map (_ => 1.0 / Math.sqrt(d))
    var converged = false

    var iteration = 1

    while (!converged) {
      println(s"iteration #$iteration...")

      val Ax = A %*% x
      var x_new = Ax.collect(::, 0)
      x_new = x_new / x_new.norm(2)

      val diff = (x_new - x).norm(2)
      println(s"difference norm is $diff")

      converged = diff < 1e-6
      iteration = iteration + 1
      x = x_new
    }

    println("converged")
    // TODO: add test that it's the 1st PC
  }

}