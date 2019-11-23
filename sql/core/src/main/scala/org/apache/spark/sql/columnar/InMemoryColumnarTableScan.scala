/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.columnar

import java.nio.ByteBuffer
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Statistics}
import org.apache.spark.sql.execution.{LeafNode, SparkPlan}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Accumulable, Accumulator, Accumulators}
import org.bridj.Pointer
import com.nativelibs4java.opencl.CLBuffer
import com.nativelibs4java.opencl.CLContext
import com.nativelibs4java.opencl.CLDevice
import com.nativelibs4java.opencl.CLKernel
import com.nativelibs4java.opencl.CLMem
import com.nativelibs4java.opencl.CLPlatform
import com.nativelibs4java.opencl.CLPlatform.DeviceFeature
import com.nativelibs4java.opencl.CLProgram
import com.nativelibs4java.opencl.CLQueue
import com.nativelibs4java.opencl.JavaCL
import com.nativelibs4java.opencl.CLPlatform.DeviceFeature
import java.nio.DoubleBuffer
import java.nio.IntBuffer

private[sql] object InMemoryRelation {
  def apply(
      useCompression: Boolean,
      batchSize: Int,
      storageLevel: StorageLevel,
      child: SparkPlan,
      tableName: Option[String]): InMemoryRelation =
    new InMemoryRelation(child.output, useCompression, batchSize, storageLevel, child, tableName)()
}

private[sql] case class ComputeTimeStats(var t_rowsIteration: Long,
    var t_buildColumnBuilders: Long, var t_createContext: Long, var t_compileKernel: Long,
    var t_buildInputBuffer: Long, var t_dataTransfer: Long, var t_deviceExecute: Long,
    var t_deviceTotal: Long, var t_cacheBuilding: Long,
    var n_rows: Long, var n_host2device: Long, var n_device2host: Long,
    var dataTransferThroughput: Double) {
    val columnsHeader = "Rows\tDevice\tIter\tTotal"
    override def toString: String = n_rows + "\t" + t_deviceTotal + "\t" + t_rowsIteration + "\t" + t_cacheBuilding
}

private[sql] case class CachedBatch(buffers: Array[Array[Byte]], stats: InternalRow)

private[sql] case class InMemoryRelation(
    output: Seq[Attribute],
    useCompression: Boolean,
    batchSize: Int,
    storageLevel: StorageLevel,
    child: SparkPlan,
    tableName: Option[String])(
    private var _cachedColumnBuffers: RDD[CachedBatch] = null,
    private var _statistics: Statistics = null,
    private var _batchStats: Accumulable[ArrayBuffer[InternalRow], InternalRow] = null)
  extends LogicalPlan with MultiInstanceRelation {

  private val batchStats: Accumulable[ArrayBuffer[InternalRow], InternalRow] =
    if (_batchStats == null) {
      child.sqlContext.sparkContext.accumulableCollection(ArrayBuffer.empty[InternalRow])
    } else {
      _batchStats
    }

  val partitionStatistics = new PartitionStatistics(output)

  private def computeSizeInBytes = {
    val sizeOfRow: Expression =
      BindReferences.bindReference(
        output.map(a => partitionStatistics.forAttribute(a).sizeInBytes).reduce(Add),
        partitionStatistics.schema)

    batchStats.value.map(row => sizeOfRow.eval(row).asInstanceOf[Long]).sum
  }

  // Statistics propagation contracts:
  // 1. Non-null `_statistics` must reflect the actual statistics of the underlying data
  // 2. Only propagate statistics when `_statistics` is non-null
  private def statisticsToBePropagated = if (_statistics == null) {
    val updatedStats = statistics
    if (_statistics == null) null else updatedStats
  } else {
    _statistics
  }

  override def statistics: Statistics = {
    if (_statistics == null) {
      if (batchStats.value.isEmpty) {
        // Underlying columnar RDD hasn't been materialized, no useful statistics information
        // available, return the default statistics.
        Statistics(sizeInBytes = child.sqlContext.conf.defaultSizeInBytes)
      } else {
        // Underlying columnar RDD has been materialized, required information has also been
        // collected via the `batchStats` accumulator, compute the final statistics,
        // and update `_statistics`.
        _statistics = Statistics(sizeInBytes = computeSizeInBytes)
        _statistics
      }
    } else {
      // Pre-computed statistics
      _statistics
    }
  }

  // If the cached column buffers were not passed in, we calculate them in the constructor.
  // As in Spark, the actual work of caching is lazy.
  if (_cachedColumnBuffers == null) {
    buildBuffers()
  }

  def recache(): Unit = {
    _cachedColumnBuffers.unpersist()
    _cachedColumnBuffers = null
    buildBuffers()
  }

  private def buildBuffers(): Unit = {
    val output = child.output
    val cached = child.execute().mapPartitions { rowIterator =>
          var context: CLContext = null
          var program: CLProgram = null
          var kernel: CLKernel = null
          var blockSize: Int = 0
          var contextCreatorThread: Thread = null
          var cacheBuildStats = ComputeTimeStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0.0)
          
          println(cacheBuildStats.columnsHeader)
          
          if(org.apache.spark.sql.CustomSettings.useOpenCL) {
            contextCreatorThread = new Thread() {
              override def run {
                val t1_context = System.nanoTime()
                context = JavaCL.createBestContext(DeviceFeature.GPU)
                //println("Context: " + context)
                /*
                val platforms = JavaCL.listGPUPoweredPlatforms()
                platforms.map { p => 
                  //println("platform: " + p.getName)
                  val devices = p.listGPUDevices(true)
                   devices.map { d =>
                    //println("Device: " + d.getName() + ", max compute units " + d.getMaxComputeUnits())
                    if(p.getName().toUpperCase().contains("NVIDIA") && d.getName().toUpperCase().contains("TITAN")) {
                      context.release()
                      context = d.getPlatform().createContext(null, d)
                    }
                    else if(p.getName().toUpperCase().contains("NVIDIA")) {
                      context.release()
                      context = d.getPlatform().createContext(null, d)
                    }
                  }
                }
                */
                //println("new context: " + context)
                val t2_context = System.nanoTime()
                cacheBuildStats.t_createContext = (t2_context-t1_context)/1000
                //println("Time to create context: " + (t2_context-t1_context)/1000 + "ms")
                val t1_compile = System.nanoTime()
                val src = """
                #ifdef CONFIG_USE_DOUBLE
                  #if defined(cl_khr_fp64)  // Khronos extension available?
                    #pragma OPENCL EXTENSION cl_khr_fp64 : enable
                    #define DOUBLE_SUPPORT_AVAILABLE
                  #elif defined(cl_amd_fp64)  // AMD extension available?
                    #pragma OPENCL EXTENSION cl_amd_fp64 : enable
                    #define DOUBLE_SUPPORT_AVAILABLE
                  #endif
                #endif // CONFIG_USE_DOUBLE
                #if defined(DOUBLE_SUPPORT_AVAILABLE)
                  // double
                  typedef double real_t;
                  typedef double2 real2_t;
                  typedef double3 real3_t;
                  typedef double4 real4_t;
                  typedef double8 real8_t;
                  typedef double16 real16_t;
                  #define PI 3.14159265358979323846
                #else
                  // float
                  typedef float real_t;
                  typedef float2 real2_t;
                  typedef float3 real3_t;
                  typedef float4 real4_t;
                  typedef float8 real8_t;
                  typedef float16 real16_t;
                  #define PI 3.14159265359f
              #endif
              __kernel void computeComplexExpression(
                 __global const real_t* a,
                 __global const real_t* b,
                 __global real_t* output, int dataSize)
                 {
                   int i = get_global_id(0);
                   if(i < dataSize)
                      output[i] = 3*sin(a[i]) + 4*cos(b[i]);
                 }
              __kernel void computeExpression(
                 __global const int* a,
                 __global const int* b,
                 __global int* output, int dataSize)
                 {
                   int i = get_global_id(0);
                   if(i < dataSize)
                      output[i] = 2*a[i] + 4*b[i];
                 }

                """
            
                program = context.createProgram(src)
                                 .defineMacro("CONFIG_USE_DOUBLE", "1")
                                 .build
                kernel = program.createKernel("computeExpression")
                val t2_compile = System.nanoTime()
                cacheBuildStats.t_compileKernel = (t2_compile-t1_compile)/1000
                //println("Time to compile the OpenCL kernel: " + (t2_compile-t1_compile)/1000 + "ms")
                //println("kernel workgroup size: " + kernel.getWorkGroupSize())
                //val sizes = context.getDevices()(0).getMaxWorkItemSizes()
                //println("Device: " + context.getDevices()(0).getName + ", workItemSizes: " + sizes(0) + ", " + sizes(1) + ", " + sizes(2))
                blockSize = context.getDevices()(0).getMaxWorkGroupSize().intValue()
              }
            }
            contextCreatorThread.start()
          }
      new Iterator[CachedBatch] {
        def next(): CachedBatch = {
          val t1_cacheBuild = System.nanoTime()
          val columnBuilders = output.map { attribute =>
            ColumnBuilder(attribute.dataType, batchSize, attribute.name, useCompression)
          }.toArray

          val t1_consumeRows = System.nanoTime()
          var rowCount = 0
          var numCols = 0
          while (rowIterator.hasNext && rowCount < batchSize) {
            val row = rowIterator.next()
            // Added for SPARK-6082. This assertion can be useful for scenarios when something
            // like Hive TRANSFORM is used. The external data generation script used in TRANSFORM
            // may result malformed rows, causing ArrayIndexOutOfBoundsException, which is somewhat
            // hard to decipher.
            numCols = row.numFields
            if(!org.apache.spark.sql.CustomSettings.useOpenCL) {
              assert(
                row.numFields == columnBuilders.size,
                s"Row column number mismatch, expected ${output.size} columns, " +
                s"but got ${row.numFields}." +
                s"\nRow content: $row")
            //}

              var i = 0
              while (i < numCols) {
                columnBuilders(i).appendFrom(row, i)
                i += 1
              }
            }
            rowCount += 1
          }
          val t2_consumeRows = System.nanoTime()
          cacheBuildStats.n_rows = rowCount
          cacheBuildStats.t_buildColumnBuilders = (t1_consumeRows - t1_cacheBuild)/1000
          cacheBuildStats.t_rowsIteration = (t2_consumeRows - t1_consumeRows)/1000
          //println(s"Time to iterate through ${rowCount} rows: " + (t2_consumeRows - t1_consumeRows)/1000 + "ms")
          //println("Time to build columnBuilders: " + (t1_consumeRows - t1_cacheBuild)/1000 + "ms")

          if(org.apache.spark.sql.CustomSettings.useOpenCL) {
            contextCreatorThread.join()
            val dataSize: java.lang.Integer = batchSize // rowCount

            val t1_dataGen = System.nanoTime
            // Copy from source buffers instead of getting values from rows
            // Simulation of BatchRow instead of InternalRow
            columnBuilders(0).underlyingBuffer.position(4).asInstanceOf[ByteBuffer].put(org.apache.spark.sql.CustomSettings.keys.array(), org.apache.spark.sql.CustomSettings.keys.position(), dataSize*4)
            columnBuilders(1).underlyingBuffer.position(4).asInstanceOf[ByteBuffer].put(org.apache.spark.sql.CustomSettings.values.array(), org.apache.spark.sql.CustomSettings.values.position(), dataSize*4)

            val aBuffer = columnBuilders(0).underlyingBuffer.position(4).asInstanceOf[ByteBuffer]
            val aBytes = new Array[Byte](dataSize*4)
            aBuffer.get(aBytes)
            val bBuffer = columnBuilders(1).underlyingBuffer.position(4).asInstanceOf[ByteBuffer]
            val bBytes = new Array[Byte](dataSize*4)
            bBuffer.get(bBytes)
            val t2_dataGen = System.nanoTime
            cacheBuildStats.t_buildInputBuffer = (t2_dataGen - t1_dataGen)/1000
            //println("Columnar input buffer building time: " + (t2_dataGen - t1_dataGen)/1000 + "ms")
            
            val queue = context.createDefaultQueue()
            val numThreads: Int = ((dataSize-1)/blockSize + 1)*blockSize
            //println("dataSize: " + dataSize + ", numThreads: " + numThreads + ", blockSize: " + blockSize)
            val t1_g = System.nanoTime()
            val memIn1 = context.createByteBuffer(CLMem.Usage.Input, ByteBuffer.wrap(aBytes), true)
            val memIn2 = context.createByteBuffer(CLMem.Usage.Input, ByteBuffer.wrap(bBytes), true)
            val memOut = context.createBuffer(CLMem.Usage.Output, classOf[java.lang.Byte], 4*dataSize.longValue)
            val t_dataXfr1_g = System.nanoTime()
            // Bind these memory objects to the arguments of the kernel
            kernel.setArgs(memIn1, memIn2, memOut, dataSize)
            kernel.enqueueNDRange(queue, Array(numThreads), Array(blockSize))
            queue.finish()
            
            val t_execute_g = System.nanoTime()
            val outBuf = memOut.read(queue)
            val outBuffer = outBuf.getByteBuffer
            columnBuilders(numCols).underlyingBuffer.put(outBuffer)
            val t2_g = System.nanoTime()
            memIn1.release()
            memIn2.release()
            memOut.release()
            queue.release()
            cacheBuildStats.t_dataTransfer = ((t_dataXfr1_g - t1_g) + (t2_g - t_execute_g))/1000
            cacheBuildStats.t_deviceExecute =  (t_execute_g - t_dataXfr1_g)/1000
            cacheBuildStats.t_deviceTotal = (t2_g-t1_g)/1000
            cacheBuildStats.n_host2device = (aBytes.length + bBytes.length)
            cacheBuildStats.n_host2device = outBuffer.position
            cacheBuildStats.dataTransferThroughput = 8*1000000000.0/(1<<30)*(aBytes.length + bBytes.length + outBuffer.position)/((t_dataXfr1_g - t1_g) + (t2_g - t_execute_g))
            /*
            println("Device data transfer time: " + ((t_dataXfr1_g - t1_g) + (t2_g - t_execute_g))/1000
                  + "ms, execute time: " + (t_execute_g - t_dataXfr1_g)/1000 + "ms"
                  +", Device time diff: " + (t2_g-t1_g)/1000 + " ms")
            println("Data transfer: host --> device: " + (aBytes.length + bBytes.length))
            println("Data transfer: device --> host: " + outBuffer.position)
            println("Data transfer throughput: " + 8*1000000000.0/(1<<30)*(aBytes.length + bBytes.length + outBuffer.position)/((t_dataXfr1_g - t1_g) + (t2_g - t_execute_g)) + " Gbps")
            */
          }

          val stats = InternalRow.fromSeq(columnBuilders.map(_.columnStats.collectedStatistics)
                        .flatMap(_.values))

          batchStats += stats
          val ret = CachedBatch(columnBuilders.map(_.build().array()), stats)
          val t2_cacheBuild = System.nanoTime()
          cacheBuildStats.t_cacheBuilding = (t2_cacheBuild - t1_cacheBuild)/1000
          //println("Total time to build cache: " + (t2_cacheBuild - t1_cacheBuild)/1000 + "ms")
          println(cacheBuildStats)
          ret
        }

        def hasNext: Boolean = rowIterator.hasNext
      }
    }.persist(storageLevel)

    cached.setName(tableName.map(n => s"In-memory table $n").getOrElse(child.toString))
    _cachedColumnBuffers = cached
  }

  def withOutput(newOutput: Seq[Attribute]): InMemoryRelation = {
    InMemoryRelation(
      newOutput, useCompression, batchSize, storageLevel, child, tableName)(
      _cachedColumnBuffers, statisticsToBePropagated, batchStats)
  }

  override def children: Seq[LogicalPlan] = Seq.empty

  override def newInstance(): this.type = {
    new InMemoryRelation(
      output.map(_.newInstance()),
      useCompression,
      batchSize,
      storageLevel,
      child,
      tableName)(
      _cachedColumnBuffers,
      statisticsToBePropagated,
      batchStats).asInstanceOf[this.type]
  }

  def cachedColumnBuffers: RDD[CachedBatch] = _cachedColumnBuffers

  override protected def otherCopyArgs: Seq[AnyRef] =
    Seq(_cachedColumnBuffers, statisticsToBePropagated, batchStats)

  private[sql] def uncache(blocking: Boolean): Unit = {
    Accumulators.remove(batchStats.id)
    cachedColumnBuffers.unpersist(blocking)
    _cachedColumnBuffers = null
  }
}

private[sql] case class InMemoryColumnarTableScan(
    attributes: Seq[Attribute],
    predicates: Seq[Expression],
    relation: InMemoryRelation)
  extends LeafNode {

  override def output: Seq[Attribute] = attributes

  private def statsFor(a: Attribute) = relation.partitionStatistics.forAttribute(a)

  // Returned filter predicate should return false iff it is impossible for the input expression
  // to evaluate to `true' based on statistics collected about this partition batch.
  val buildFilter: PartialFunction[Expression, Expression] = {
    case And(lhs: Expression, rhs: Expression)
      if buildFilter.isDefinedAt(lhs) || buildFilter.isDefinedAt(rhs) =>
      (buildFilter.lift(lhs) ++ buildFilter.lift(rhs)).reduce(_ && _)

    case Or(lhs: Expression, rhs: Expression)
      if buildFilter.isDefinedAt(lhs) && buildFilter.isDefinedAt(rhs) =>
      buildFilter(lhs) || buildFilter(rhs)

    case EqualTo(a: AttributeReference, l: Literal) =>
      statsFor(a).lowerBound <= l && l <= statsFor(a).upperBound
    case EqualTo(l: Literal, a: AttributeReference) =>
      statsFor(a).lowerBound <= l && l <= statsFor(a).upperBound

    case LessThan(a: AttributeReference, l: Literal) => statsFor(a).lowerBound < l
    case LessThan(l: Literal, a: AttributeReference) => l < statsFor(a).upperBound

    case LessThanOrEqual(a: AttributeReference, l: Literal) => statsFor(a).lowerBound <= l
    case LessThanOrEqual(l: Literal, a: AttributeReference) => l <= statsFor(a).upperBound

    case GreaterThan(a: AttributeReference, l: Literal) => l < statsFor(a).upperBound
    case GreaterThan(l: Literal, a: AttributeReference) => statsFor(a).lowerBound < l

    case GreaterThanOrEqual(a: AttributeReference, l: Literal) => l <= statsFor(a).upperBound
    case GreaterThanOrEqual(l: Literal, a: AttributeReference) => statsFor(a).lowerBound <= l

    case IsNull(a: Attribute) => statsFor(a).nullCount > 0
    case IsNotNull(a: Attribute) => statsFor(a).count - statsFor(a).nullCount > 0
  }

  val partitionFilters: Seq[Expression] = {
    predicates.flatMap { p =>
      val filter = buildFilter.lift(p)
      val boundFilter =
        filter.map(
          BindReferences.bindReference(
            _,
            relation.partitionStatistics.schema,
            allowFailures = true))

      boundFilter.foreach(_ =>
        filter.foreach(f => logInfo(s"Predicate $p generates partition filter: $f")))

      // If the filter can't be resolved then we are missing required statistics.
      boundFilter.filter(_.resolved)
    }
  }

  lazy val enableAccumulators: Boolean =
    sqlContext.getConf("spark.sql.inMemoryTableScanStatistics.enable", "false").toBoolean

  // Accumulators used for testing purposes
  lazy val readPartitions: Accumulator[Int] = sparkContext.accumulator(0)
  lazy val readBatches: Accumulator[Int] = sparkContext.accumulator(0)

  private val inMemoryPartitionPruningEnabled = sqlContext.conf.inMemoryPartitionPruning

  protected override def doExecute(): RDD[InternalRow] = {
    if (enableAccumulators) {
      readPartitions.setValue(0)
      readBatches.setValue(0)
    }

    relation.cachedColumnBuffers.mapPartitions { cachedBatchIterator =>
      val partitionFilter = newPredicate(
        partitionFilters.reduceOption(And).getOrElse(Literal(true)),
        relation.partitionStatistics.schema)

      // Find the ordinals and data types of the requested columns.  If none are requested, use the
      // narrowest (the field with minimum default element size).
      val (requestedColumnIndices, requestedColumnDataTypes) = if (attributes.isEmpty) {
        val (narrowestOrdinal, narrowestDataType) =
          relation.output.zipWithIndex.map { case (a, ordinal) =>
            ordinal -> a.dataType
          } minBy { case (_, dataType) =>
            ColumnType(dataType).defaultSize
          }
        Seq(narrowestOrdinal) -> Seq(narrowestDataType)
      } else {
        attributes.map { a =>
          relation.output.indexWhere(_.exprId == a.exprId) -> a.dataType
        }.unzip
      }

      val nextRow = new SpecificMutableRow(requestedColumnDataTypes)

      def cachedBatchesToRows(cacheBatches: Iterator[CachedBatch]): Iterator[InternalRow] = {
        val rows = cacheBatches.flatMap { cachedBatch =>
          // Build column accessors
          val columnAccessors = requestedColumnIndices.map { batchColumnIndex =>
            ColumnAccessor(
              relation.output(batchColumnIndex).dataType,
              ByteBuffer.wrap(cachedBatch.buffers(batchColumnIndex)))
          }

          // Extract rows via column accessors
          new Iterator[InternalRow] {
            private[this] val rowLen = nextRow.numFields
            override def next(): InternalRow = {
              var i = 0
              while (i < rowLen) {
                columnAccessors(i).extractTo(nextRow, i)
                i += 1
              }
              if (attributes.isEmpty) InternalRow.empty else nextRow
            }

            override def hasNext: Boolean = columnAccessors(0).hasNext
          }
        }

        if (rows.hasNext && enableAccumulators) {
          readPartitions += 1
        }

        rows
      }

      // Do partition batch pruning if enabled
      val cachedBatchesToScan =
        if (inMemoryPartitionPruningEnabled) {
          cachedBatchIterator.filter { cachedBatch =>
            if (!partitionFilter(cachedBatch.stats)) {
              def statsString: String = relation.partitionStatistics.schema.zipWithIndex.map {
                case (a, i) =>
                  val value = cachedBatch.stats.get(i, a.dataType)
                  s"${a.name}: $value"
              }.mkString(", ")
              logInfo(s"Skipping partition based on stats $statsString")
              false
            } else {
              if (enableAccumulators) {
                readBatches += 1
              }
              true
            }
          }
        } else {
          cachedBatchIterator
        }

      cachedBatchesToRows(cachedBatchesToScan)
    }
  }
}
