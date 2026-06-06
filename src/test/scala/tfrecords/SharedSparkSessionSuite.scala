/**
 *  Copyright 2016 The TensorFlow Authors. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package tfrecords

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}


trait BaseSuite extends WordSpecLike with Matchers with BeforeAndAfterAll

class SharedSparkSessionSuite extends BaseSuite {
  val TF_SANDBOX_DIR = "tf-sandbox"
  val file = new File(TF_SANDBOX_DIR)
  private val warehouseDir = new File("target/spark-warehouse")

  protected var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    FileUtils.deleteQuietly(file)
    file.mkdirs()
    warehouseDir.mkdirs()
    spark = SparkSession.builder()
      .appName("gerbil-data-test")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.sql.warehouse.dir", warehouseDir.getAbsolutePath)
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try {
      FileUtils.deleteQuietly(file)
    } finally {
      if (spark != null) {
        spark.stop()
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
        spark = null
      }
      super.afterAll()
    }
  }
}
