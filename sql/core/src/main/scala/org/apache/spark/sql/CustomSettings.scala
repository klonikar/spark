package org.apache.spark.sql

import java.nio.ByteBuffer

/**
 * @author klonikar
 */
object CustomSettings {
  var useOpenCL = false
  var keys: ByteBuffer = null
  var values: ByteBuffer = null
}
