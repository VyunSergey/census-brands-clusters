package com.vyunsergey

import org.apache.spark.sql.SparkSession
import zio._

package object spark {
  type SparkEnv = Has[SparkEnv.Service]

  def createSparkSession(name: String, cores: Int): ZIO[SparkEnv, Throwable, SparkSession] =
    ZIO.accessM(_.get.createSparkSession(name, cores))
}
