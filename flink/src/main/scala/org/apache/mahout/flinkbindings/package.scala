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

package org.apache.mahout

import org.apache.flink.api.common.functions.{JoinFunction, MapFunction}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.{ExecutionEnvironment, DataSet}
import org.apache.flink.api.java.tuple.{Tuple2 => JavaTuple2 }
import org.apache.mahout.math.drm.DistributedContext
import org.apache.mahout.math.Vector

package object flinkbindings {

  type JT2[A,B] = JavaTuple2[A,B]
  type DrmTuple[K] = JT2[K, Vector]

  /**
   * Type definition for drm in Flink
   */
  type DrmDS[K] = DataSet[DrmTuple[K]]

  def mahoutLocalFlinkContext(): FlinkDistributedContext = {
    new FlinkDistributedContext(ExecutionEnvironment.createLocalEnvironment())

  }

  def mahoutRemoteFlinkContext(host: String, port: Int, dop: Int,
                                      jarFiles: Seq[String]): FlinkDistributedContext
  = {
    val ee = ExecutionEnvironment.createRemoteEnvironment(host, port, dop, jarFiles:_*)
    new FlinkDistributedContext(ee)
  }

  implicit def FlinkContext2ExecutionEnvironment(context: FlinkDistributedContext):
  ExecutionEnvironment = {
    context.env
  }

  implicit def distributedContext2ExecutionEnvironment(context: DistributedContext): ExecutionEnvironment = {
    FlinkContext2ExecutionEnvironment(distributedContext2FlinkDistriubtedContext(context))
  }

  implicit def distributedContext2FlinkDistriubtedContext(context: DistributedContext):
  FlinkDistributedContext = {
    assert(context.isInstanceOf[FlinkDistributedContext], "Context has to be a Flink context.")
    context.asInstanceOf[FlinkDistributedContext]
  }

  implicit def javaTuple2CaseClass[A,B](tuple: JT2[A,B]): (A,B) = {
    (tuple.f0, tuple.f1)
  }

  implicit def caseClass2JavaTuple[A,B](cc: (A,B)):JT2[A,B] = {
    new JavaTuple2[A,B](cc._1, cc._2)
  }

  implicit def createMapFunction[A,B](mapFunction: A => B): MapFunction[A,B] = {
    new MapFunction[A,B](){
      override def map(a: A): B = {
        mapFunction(a)
      }
    }
  }

  implicit def createJoinFunction[A,B,C](joinFunction: (A,B) => C): JoinFunction[A,B,C] = {
    new JoinFunction[A,B,C](){
      override def join(a: A, b: B): C ={
        joinFunction(a,b)
      }
    }
  }

  implicit def createKeySelector[A,B](selector: A => B): KeySelector[A,B] = {
    new KeySelector[A,B]() {
      override def getKey(a: A): B = {
        selector(a)
      }
    }
  }

  object JT{
    def apply[A,B](valueA: A, valueB: B): JT2[A,B] = { new JT2[A,B](valueA, valueB)}

    def unapply[A,B](tuple: JT2[A,B]): Option[(A,B)] = {
      Some((tuple.f0, tuple.f1))
    }
  }
}
