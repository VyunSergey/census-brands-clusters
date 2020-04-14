package com.vyunsergey

import com.vyunsergey.args._
import com.vyunsergey.conf._
import com.vyunsergey.spark._
import com.vyunsergey.struct._
import org.apache.spark.sql.functions._
import zio._
import zio.console._

object Main extends App {
  type AppEnvironment = ZEnv with Configuration with SparkEnv

  /*
   * Main program function
   */
  def run(args: List[String]): ZIO[ZEnv, Nothing, Int] = {
//    createClusters(args)
    createBrands(args)
//    createBrands(args)
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
   * Create Brands from Census cluster DataFrame
   */
  def createBrands(args: List[String]): ZIO[AppEnvironment, Throwable, Unit] = for {
    conf       <- config
    args       <- ArgumentsParser.parse(args)
    readPath    = args.flatMap(_.readPath)
    writePath   = args.flatMap(_.writePath)
    _          <- putStrLn(s"read path: $readPath")
    _          <- putStrLn(s"write path: $writePath")
    spark      <- createSparkSession("census-brands", 8)
    // read Excel Dictionary
    excel      <- SparkApp.readExcel(conf.readConf, BrandsDictSchema.schema).provide(spark)
    // filter on Column mcc_supercat
    superCat    = "Поездки, доставка, хранение"
    // filter Rules from Excel with Column mcc_supercat
    rules      <- SparkApp.filterData(excel, "mcc_supercat", Some(superCat)).provide(spark)
    // create Brands Expression as String
    expr       <- SparkApp.createBrandExpression(rules, "brand", "reg_exp").provide(spark)
    _          <- putStrLn(s"expression:\n${SparkApp.BothSubstring(expr, 1000)}")
    // read Census cluster DataFrame
    data       <- SparkApp.readData(conf.readConf, CensusSchema.schema, readPath).provide(spark)
    _          <- putStrLn(s"count: ${data.count}")
    // create Brands from Census cluster DataFrame with Brands Expression
    brands     <- SparkApp.withBrandColumn(data, "brand", rules,
      "brand", "reg_exp").provide(spark)
    _          <- putStrLn(s"count: ${brands.count}")
    _          <- putStrLn(s"schema: ${brands.printSchema()}")
    _          <- putStrLn(s"sample: ${brands.show(false)}")
    // collect Statistics with Brands on Census cluster DataFrame
    brandsStat <- SparkApp.countStatistics(brands, "mcc_supercat", "brands_part", "brand").provide(spark)
    _          <- putStrLn(s"stats: ${brandsStat.show(300, truncate = false)}")
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
    aircraft   <- SparkApp.filterData(data, "brands_part", Some("travel_2")).provide(spark)
    _          <- putStrLn(s"count: ${aircraft.count}")
    _          <- putStrLn(s"schema: ${aircraft.printSchema()}")
    _          <- putStrLn(s"sample: ${aircraft.show(false)}")
    result     <- Task(
                   aircraft
                     .withColumn("brand",
                       expr("regexp_replace(trim(mcc_name), 'Авиакомпании – ', '')"))
                     .withColumn("mcl",
                       expr("trim(lower(brand))"))
                     .withColumn("rule", expr("concat_ws(''," +
                       "'mcc_code IN (\\''," +
                       "mcc_code," +
                       "'\\') AND trim(lower(mcc_name)) regexp \\''," +
                       "mcl," +
                       "'\\'')"))
    )
    statsBrand <- SparkApp.countStatistics(result, "mcc_name", "rule", "brand").provide(spark)
    _          <- putStrLn(s"stats: ${statsBrand.show(300, truncate = false)}")
  } yield ()

  /*
   * Check Statistics on Source BigData Census DataFrame
   * Create Result cluster`s from Source BigData Census DataFrame
   */
  def createClusters(args: List[String]): ZIO[AppEnvironment, Throwable, Unit] = for {
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
