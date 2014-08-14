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

package org.apache.mahout.flinkbindings.blas

import org.apache.mahout.flinkbindings._
import scala.reflect.ClassTag
import org.apache.mahout.math.scalabindings._
import org.apache.mahout.math.Vector
import RLikeOps._

object AewB {
  def a_plus_scalar[K:ClassTag](srcA: DrmDS[K], scalar: Double): DrmDS[K] = {
    a_ew_scalar(srcA, scalar, (r, s) => r += s)
  }
  
  def a_minus_scalar[K:ClassTag](srcA: DrmDS[K], scalar: Double): DrmDS[K] = {
    a_ew_scalar(srcA, scalar, (r,s) => r -= s)
  }
  
  def scalar_minus_a[K:ClassTag](scalar: Double, srcA: DrmDS[K]): DrmDS[K] = {
    a_ew_scalar(srcA, scalar, (r,s) => s -=: r)
  }

  def a_times_scalar[K:ClassTag](srcA: DrmDS[K], scalar: Double): DrmDS[K] = {
    a_ew_scalar(srcA, scalar, (r,s) => r *= s)
  }

  def a_div_scalar[K:ClassTag](srcA: DrmDS[K], scalar: Double): DrmDS[K] = {
    a_ew_scalar(srcA, scalar, (r,s) => r /= s)
  }

  def scalar_div_a[K:ClassTag](scalar: Double, srcA: DrmDS[K]): DrmDS[K] = {
    a_ew_scalar(srcA, scalar, (r,s) => s /=: r)
  }

  def a_plus_b[K:ClassTag](srcA: DrmDS[K], srcB: DrmDS[K]): DrmDS[K] = {
    a_ew_b(srcA, srcB, (a,b) => a += b)
  }

  def a_minus_b[K:ClassTag](srcA: DrmDS[K], srcB: DrmDS[K]): DrmDS[K] = {
    a_ew_b(srcA, srcB, (a,b) => a -=b)
  }

  def a_hadamard_b[K:ClassTag](srcA: DrmDS[K], srcB:DrmDS[K]): DrmDS[K] = {
    a_ew_b(srcA, srcB, (a,b) => a *= b)
  }

  def a_eldiv_b[K:ClassTag](srcA: DrmDS[K], srcB: DrmDS[K]): DrmDS[K] = {
    a_ew_b(srcA, srcB, (a,b) => a /= b)
  }

  private def a_ew_scalar[K:ClassTag](src: DrmDS[K], scalar: Double, update: (Vector,
    Double) => Vector): DrmDS[K] = {
    src.map{
      x:DrmTuple[K] =>
        x match {
          case JT(rowIdx, vector) => JT(rowIdx, update(vector, scalar))
        }
    }
  }

  private def a_ew_b[K:ClassTag](srcA: DrmDS[K], srcB: DrmDS[K], update:(Vector,
    Vector) => Vector): DrmDS[K] = {
    srcA.join(srcB).where(0).equalTo(0)
    .`with`{
      (a: DrmTuple[K], b: DrmTuple[K]) =>
        JT(a._1, update(a._2, b._2))
    }
  }
}
