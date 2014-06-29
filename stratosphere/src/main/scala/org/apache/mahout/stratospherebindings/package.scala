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

import eu.stratosphere.api.java.functions.{KeySelector, JoinFunction, MapFunction}
import eu.stratosphere.api.java.{ExecutionEnvironment, DataSet}
import eu.stratosphere.api.java.tuple.{Tuple2 => JavaTuple2 }
import org.apache.mahout.math.drm.DistributedContext
import org.apache.mahout.math.Vector

package object stratospherebindings {

  type JT2[A,B] = JavaTuple2[A,B]
  type DrmTuple[K] = JT2[K, Vector]

  /**
   * Type definition for drm in Stratosphere
   */
  type DrmDS[K] = DataSet[DrmTuple[K]]

  def mahoutLocalStratosphereContext(): StratosphereDistributedContext = {
    new StratosphereDistributedContext(ExecutionEnvironment.createLocalEnvironment())

  }

  def mahoutRemoteStratosphereContext(host: String, port: Int, dop: Int,
                                      jarFiles: Seq[String]): StratosphereDistributedContext
  = {
    val ee = ExecutionEnvironment.createRemoteEnvironment(host, port, dop, jarFiles:_*)
    new StratosphereDistributedContext(ee)
  }

  implicit def stratosphereContext2ExecutionEnvironment(context: StratosphereDistributedContext):
  ExecutionEnvironment = {
    context.env
  }

  implicit def distributedContext2ExecutionEnvironment(context: DistributedContext): ExecutionEnvironment = {
    stratosphereContext2ExecutionEnvironment(distributedContext2StratosphereDistriubtedContext(context))
  }

  implicit def distributedContext2StratosphereDistriubtedContext(context: DistributedContext):
  StratosphereDistributedContext = {
    assert(context.isInstanceOf[StratosphereDistributedContext], "Context has to be a Stratosphere context.")
    context.asInstanceOf[StratosphereDistributedContext]
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
