package com.vyunsergey.spark

import org.apache.spark.sql.SparkSession
import zio._

object SparkEnv {

  trait Service {
    def createSparkSession(name: String, cores: Int): Task[SparkSession]
  }

  trait Live extends SparkEnv.Service {
    def createSparkSession(name: String, cores: Int): Task[SparkSession] = for {
      spark <- Task(
        SparkSession.builder()
          .appName(name)
          .master(s"local[$cores]")
          .config("spark.driver.memory", "16G")
          .config("spark.driver.memoryOverhead", "2G")
          .config("spark.driver.maxResultSize", "6G")
          .config("spark.executor.memory", "4G")
          .config("spark.executor.memoryOverhead", "1G")
          .config("spark.executor.extraJavaOptions",
            "-Xss1024m -XX:+UseG1GC -XX:+PrintGCDetails -XX+CrashOnOutOfMemoryError -XX:+CMSClassUnloadingEnabled")
          .config("spark.logConf", value = true)
          .config("spark.ui.enabled", value = true)
          .config("spark.shuffle.service.enabled", value = true)
          .config("spark.sql.shuffle.partitions", cores)
          .config("spark.default.parallelism", cores)
          .config("spark.debug.maxToStringFields", 300)
          .getOrCreate()
      )
      _ <- Task(spark.sparkContext.setLogLevel("WARN"))
    } yield spark
  }

  val live: ZLayer[Any, Nothing, SparkEnv] = ZLayer.succeed(new Live {})
}
