package trial

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * @author klonikar
 */
object TungstenDF {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("SparkLR").setMaster("local")
    val sc = new SparkContext(sparkConf)
    //val numSlices = if (args.length > 0) args(0).toInt else 2
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val batchSize = 1000000
    sqlContext.setConf("spark.sql.inMemoryColumnarStorage.compressed", "false")
    sqlContext.setConf("spark.sql.inMemoryColumnarStorage.batchSize", batchSize.toString())
    
    var size = 20000000
    val numPartitions = 5
    val numBatchesInPartition = size/numPartitions/batchSize
    if(org.apache.spark.sql.CustomSettings.useOpenCL) {
      val t1_buildBuffer = System.nanoTime
      org.apache.spark.sql.CustomSettings.keys = ByteBuffer.allocate(size*4).order(ByteOrder.nativeOrder())
      org.apache.spark.sql.CustomSettings.values = ByteBuffer.allocate(size*4).order(ByteOrder.nativeOrder())
      (1 to size).foreach {
        x => 
          org.apache.spark.sql.CustomSettings.keys.putInt(x)
          org.apache.spark.sql.CustomSettings.values.putInt(x*x + 1)
      }
      org.apache.spark.sql.CustomSettings.keys.flip()
      org.apache.spark.sql.CustomSettings.values.flip()
      val t2_buildBuffer = System.nanoTime
      println(s"Time to build columnar buffers: ${(t2_buildBuffer-t1_buildBuffer)/1000} mus")
      //size = 10
    }
    val data = sc.parallelize(1 to size, numPartitions).map {
      x => (x, x*x + 1)
    }.toDF("key", "value")
    val data1 = data.select($"key", $"value", $"key"*2 + $"value"*4 as "newVal").cache
    data.explain(true)
    data1.explain(true)
    //data.show()
    val t1 = System.nanoTime()
    data1.show()
    //data1.write.parquet("TungstenDF.parquet")
    val t2 = System.nanoTime()
    println("Time to complete the job: " + (t2-t1)/1000000 + "ms")
    
    sc.stop()
  }

}
