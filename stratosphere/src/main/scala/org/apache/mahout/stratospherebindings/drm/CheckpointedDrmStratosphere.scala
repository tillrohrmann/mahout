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

package org.apache.mahout.stratospherebindings.drm

import eu.stratosphere.api.java.functions.MapFunction
import eu.stratosphere.hadoopcompatibility.mapred.{HadoopOutputFormat}
import org.apache.hadoop.io.{LongWritable, Text, IntWritable, Writable}
import org.apache.hadoop.mapred.{JobConf, SequenceFileOutputFormat}
import org.apache.mahout.math.Vector
import org.apache.mahout.math.drm.{CacheHint, CheckpointedDrm}
import org.apache.mahout.math.{VectorWritable, Matrix}
import org.apache.mahout.stratospherebindings._
import scala.reflect.ClassTag
import scala.util.Random
import scala.reflect.classTag

class CheckpointedDrmStratosphere[K: ClassTag](val ds: DrmDS[K],
                                               override protected[mahout] val context: StratosphereDistributedContext,
                                               override protected[mahout] val partitioningTag: Long = Random.nextLong())
  extends CheckpointedDrm[K] {

  override val ncol: Int = -1
  override val nrow: Long = -1

  override def collect: Matrix = {
    null
  }

  override def writeDRM(path: String) {
    val keyTag = implicitly[ClassTag[K]]
    val I = classTag[Int]
    val S = classTag[String]
    val L = classTag[Long]

    implicit val key2WritableFunc: (K) => Writable = keyTag match {
      case I => (x: K) => new IntWritable(x.asInstanceOf[Int])
      case S => (x: K) => new Text(x.asInstanceOf[String])
      case L => (x: K) => new LongWritable(x.asInstanceOf[Long])
      case _ =>
        if(classOf[Writable].isAssignableFrom(keyTag.runtimeClass)) (x:K) => x.asInstanceOf[Writable]
        else throw new IllegalArgumentException(s"Cannot convert type $keyTag to Writable.")
    }
    val jobConf = new JobConf()
    val outputFormat = new HadoopOutputFormat[Writable, VectorWritable](new SequenceFileOutputFormat[Writable,
      VectorWritable](), jobConf)

    class Convert2Writable[T <: Vector] extends MapFunction[JT2[K, T], JT2[Writable, VectorWritable]]{
      override def map(x: JT2[K, T]): JT2[Writable, VectorWritable] = {
        JT(key2WritableFunc(x._1), new VectorWritable(x._2))
      }
    }

    val wvw = ds.map{
      new Convert2Writable[Vector]
    }

    wvw.output(outputFormat)

    context.execute("Write DRM.")
  }

  override def uncache(){
    // Stratosphere currently does not support caching
  }

  override def checkpoint(cacheHint: CacheHint.CacheHint = CacheHint.MEMORY_ONLY): CheckpointedDrmStratosphere[K] = {
    this
  }


}
