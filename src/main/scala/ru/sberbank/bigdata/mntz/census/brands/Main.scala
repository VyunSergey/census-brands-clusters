package ru.sberbank.bigdata.mntz.census.brands

import ru.sberbank.bigdata.mntz.census.brands.args._
import ru.sberbank.bigdata.mntz.census.brands.conf._
import ru.sberbank.bigdata.mntz.census.brands.spark._
import ru.sberbank.bigdata.mntz.census.brands.struct._
import zio._
import zio.console._

object Main extends App {
  type AppEnvironment = ZEnv with Configuration with SparkEnv

  /*
   * Main program function
   */
  def run(args: List[String]): ZIO[ZEnv, Nothing, Int] = {
//    program(args)
//    checkResult(args)
    showDict(args)
      .provideSomeLayer[ZEnv](Configuration.live ++ SparkEnv.live)
      .foldM(
        error => putStrLn(s"Execution failed with error: $error") *> ZIO.succeed(1),
        _ => ZIO.succeed(0)
      )
  }

  /*
   * Load Excel Dictionary with specified schema:
   * root
   *  |-- num: integer (nullable = true)
   *  |-- reg_exp: string (nullable = true)
   *  |-- brand: string (nullable = true)
   *  |-- mcc_supercat: string (nullable = true)
   *  |-- comment: string (nullable = true)
   *
   * Check Statistics on Excel Dictionary
   */
  def showDict(args: List[String]): ZIO[AppEnvironment, Throwable, Unit] = for {
    conf       <- config
    args       <- ArgumentsParser.parse(args)
    readPath    = args.flatMap(_.readPath)
    writePath   = args.flatMap(_.writePath)
    _          <- putStrLn(s"read path: $readPath")
    _          <- putStrLn(s"write path: $writePath")
    spark      <- createSparkSession("census-brands", 8)
    excel      <- SparkApp.readExcel(conf.readConf, BrandsDictSchema.schema, readPath).provide(spark)
    _          <- putStrLn(s"count: ${excel.count}")
    _          <- putStrLn(s"schema: ${excel.printSchema()}")
    _          <- putStrLn(s"sample: ${excel.show(false)}")
    statsExcel <- SparkApp.countStatistics(excel, "mcc_supercat").provide(spark)
    _          <- putStrLn(s"stats: ${statsExcel.show(300, truncate = false)}")
  } yield ()

  /*
   * Check Statistics on Result DataFrame cluster`s from Source BigData Census DataFrame
   */
  def checkResult(args: List[String]): ZIO[AppEnvironment, Throwable, Unit] = for {
    conf       <- config
    args       <- ArgumentsParser.parse(args)
    readPath    = args.flatMap(_.readPath)
    writePath   = args.flatMap(_.writePath)
    _          <- putStrLn(s"read path: $readPath")
    _          <- putStrLn(s"write path: $writePath")
    spark      <- createSparkSession("census-brands", 8)
    data       <- SparkApp.readData(conf.readConf, CensusSchema.schema, readPath).provide(spark)
    _          <- putStrLn(s"count: ${data.count}")
    _          <- putStrLn(s"schema: ${data.printSchema()}")
    _          <- putStrLn(s"sample: ${data.show(false)}")
    statsBrand <- SparkApp.countStatistics(data, "mcc_supercat", "brands_part").provide(spark)
    _          <- putStrLn(s"stats: ${statsBrand.show(300, truncate = false)}")
  } yield ()

  /*
   * Check Statistics on Source BigData Census DataFrame
   * Create Result cluster`s from Source BigData Census DataFrame
   */
  def program(args: List[String]): ZIO[AppEnvironment, Throwable, Unit] = for {
    conf       <- config
    args       <- ArgumentsParser.parse(args)
    readPath    = args.flatMap(_.readPath)
    writePath   = args.flatMap(_.writePath)
    _          <- putStrLn(s"read path: $readPath")
    _          <- putStrLn(s"write path: $writePath")
    spark      <- createSparkSession("census-brands", 8)
    text       <- SparkApp.readAsText(conf.readConf, readPath).provide(spark)
    _          <- putStrLn(s"count: ${text.count}")
    _          <- putStrLn(s"schema: ${text.printSchema()}")
    _          <- putStrLn(s"sample: ${text.show(false)}")
    first      <- SparkApp.getFirst(text).provide(spark)
    _          <- putStrLn(s"first: ${first.mkString}")
    text2      <- SparkApp.filterRow(text, first).provide(spark)
    rdd        <- SparkApp.splitLineRDD(text2.rdd, conf.readConf.fileConf.separator).provide(spark)
    data       <- SparkApp.createDataFrame(rdd, CensusSchema.schema).provide(spark)
    _          <- putStrLn(s"count: ${data.count}")
    _          <- putStrLn(s"schema: ${data.printSchema()}")
    _          <- putStrLn(s"partitions: ${data.rdd.getNumPartitions}")
    _          <- putStrLn(s"sample: ${data.show(false)}")
    statsData  <- SparkApp.countStatistics(data, "mcc_supercat").provide(spark)
    _          <- putStrLn(s"stats: ${statsData.show(100, truncate = false)}")
    brandsData <- SparkApp.transformData(data).provide(spark)
    _          <- putStrLn(s"count: ${brandsData.count}")
    _          <- putStrLn(s"schema: ${brandsData.printSchema()}")
    _          <- putStrLn(s"partitions: ${brandsData.rdd.getNumPartitions}")
    _          <- putStrLn(s"sample: ${brandsData.show(false)}")
    statsBrand <- SparkApp.countStatistics(brandsData, "mcc_supercat", "brands_part").provide(spark)
    _          <- putStrLn(s"stats: ${statsBrand.show(300, truncate = false)}")
    _          <- SparkApp.writeData(conf.writeConf, brandsData, writePath, List("brands_part")).provide(spark)
    _          <- putStrLn("============DONE SAVING DATA============")
  } yield ()
}
