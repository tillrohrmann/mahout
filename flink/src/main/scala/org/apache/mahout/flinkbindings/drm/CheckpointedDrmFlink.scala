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

package org.apache.mahout.flinkbindings.drm

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.hadoopcompatibility.mapred.{HadoopOutputFormat}
import org.apache.hadoop.io.{LongWritable, Text, IntWritable, Writable}
import org.apache.hadoop.mapred.{JobConf, SequenceFileOutputFormat}
import org.apache.mahout.math.Vector
import org.apache.mahout.math.drm.{CacheHint, CheckpointedDrm}
import org.apache.mahout.math.{VectorWritable, Matrix}
import org.apache.mahout.flinkbindings._
import scala.reflect.ClassTag
import scala.util.Random
import scala.reflect.classTag

class CheckpointedDrmFlink[K: ClassTag](val ds: DrmDS[K],
                                               override val context: FlinkDistributedContext,
                                               override protected[mahout] val partitioningTag: Long = Random.nextLong())
  extends CheckpointedDrm[K] {

  override val ncol: Int = -1
  override val nrow: Long = -1

  override def canHaveMissingRows = false

  override def collect: Matrix = {
    null
  }

  override def writeDRM(path: String) {
    val jobConf = new JobConf()
    val outputFormat = new HadoopOutputFormat[Writable, VectorWritable](new SequenceFileOutputFormat[Writable,
      VectorWritable](), jobConf)

    val wvw = ds.map{
      new Convert2Writable[Vector, K](Convert2Writable.createMappingFunction[K])
    }

    wvw.output(outputFormat)

    context.execute("Write DRM.")
  }

  override def uncache() = {
    // Stratosphere currently does not support caching
    this
  }

  override def checkpoint(cacheHint: CacheHint.CacheHint = CacheHint.MEMORY_ONLY): CheckpointedDrmFlink[K] = {
    this
  }
}

class Convert2Writable[T<: Vector, K](val mappingFunction: K => Writable) extends MapFunction[JT2[K, T], JT2[Writable,
  VectorWritable]]{

  override def map(x: JT2[K, T]) = {
    JT(mappingFunction(x._1), new VectorWritable(x._2))
  }
}

object Convert2Writable{
  val I = classTag[Int]
  val S = classTag[String]
  val L = classTag[Long]

  def createMappingFunction[K: ClassTag] = {
    val tag = implicitly[ClassTag[K]]
    tag match {
      case I => (x: K) => new IntWritable(x.asInstanceOf[Int])
      case S => (x: K) => new Text(x.asInstanceOf[String])
      case L => (x: K) => new LongWritable(x.asInstanceOf[Long])
      case _ => throw new IllegalArgumentException(s"Cannot convert type $tag to Writable.")
    }
  }
}
