package com.vyunsergey

import com.vyunsergey.args._
import com.vyunsergey.conf._
import com.vyunsergey.spark._
import com.vyunsergey.struct._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import zio._
import zio.console._

import scala.collection.mutable

object Main extends App {
  type AppEnvironment = ZEnv with Configuration with SparkEnv

  /*
   * Main program function
   */
  def run(args: List[String]): ZIO[ZEnv, Nothing, Int] = {
    createClusters(args)
//    checkRules(args)
//    checkResult(args)
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
  def checkResult(args: List[String]): ZIO[AppEnvironment, Throwable, Unit] = for {
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
    superCat    = "Всё для дома"
    // filter Rules from Excel with Column mcc_supercat
    rules      <- SparkApp.filterData(excel, "mcc_supercat", Some(superCat)).provide(spark).map(_.cache)
    // create Brands Expression as String
    expr       <- SparkApp.createBrandExpression(rules, "brand", "reg_exp").provide(spark)
    _          <- putStrLn(s"expression:\n${SparkApp.BothSubstring(expr, 1000)}")
    // read Census cluster DataFrame
    data       <- SparkApp.readData(conf.readConf, CensusSchema.schema, readPath).provide(spark)
    _          <- putStrLn(s"count: ${data.count}")
    // create Brands from Census cluster DataFrame with Brands Expression
    udf        <- Task(udf((x: mutable.WrappedArray[String]) => x.length))
    brands     <- SparkApp.withBrandColumn(data, "brand", rules, "brand", "reg_exp").provide(spark)
                   .map(_.filter(col("brand").isNull)
                         .withColumn("has_brand", col("brand").isNotNull)
                         .withColumn("mcc_clr", substring(SparkApp.clearColumn(col("mcc_name")), 0, 30))
                         .withColumn("merchant_clr", substring(SparkApp.clearColumn(col("merchant_nm")), 0, 30))
                         .withColumn("words", udf(split(col("merchant_clr"), " ")))
                         .cache)
    _          <- putStrLn(s"count: ${brands.count}")
    _          <- putStrLn(s"schema: ${brands.printSchema()}")
    _          <- putStrLn(s"sample: ${brands.filter(col("brand").isNull).show(50, truncate = false)}")
//    checkBrand  = brands.filter(col("brand") === "OneTwoTrip.com")
//    _          <- putStrLn(s"current brand: ${checkBrand.show(50, truncate = false)}")
//    checkGroup <- SparkApp.countGroupBy(checkBrand, "mcc_supercat", "merchant_clr", "merchant_id", "merchant_city_nm").provide(spark)
//    _          <- putStrLn(s"stats: ${checkGroup.show(300, truncate = false)}")
    // collect Statistics with Brands on Census cluster DataFrame
    brandsStat <- SparkApp.countStatistics(brands, "mcc_supercat", "brands_part", "merchant_city_nm", "has_brand").provide(spark)
    _          <- putStrLn(s"stats: ${brandsStat.show(50, truncate = false)}")
    brandsGrp  <- SparkApp.countGroupBy(brands, "mcc_supercat", "brands_part", "merchant_city_nm", "has_brand").provide(spark)
                  //.map(_.filter(col("count") > 100)
                  //      .orderBy(col("words").asc, col("count").desc)
                  //)
    _          <- putStrLn(s"stats: ${brandsGrp.show(50, truncate = false)}")
    brandsMsc  <- Task(brands
                       .withColumn("brand", when(col("brand") === "НЕ БРЕНД", "")
                         .when(col("brand") === "ИП", "").otherwise(col("brand")))
                       .withColumn("brand", coalesce(col("brand"), lit("")))
                       .withColumn("merchant_nm", coalesce(col("merchant_nm"), lit("")))
                       .withColumn("terminal_id", coalesce(col("terminal_id"), lit("")))
                       .filter(col("merchant_city_nm").isin("MOSCOW", "MOSKVA", "G MOSKVA", "MASKVA"))
                       .filter(col("latitude_mt") =!= "\\N" && col("latitude_mt").isNotNull)
                       .filter(col("longitude_mt") =!= "\\N" && col("longitude_mt").isNotNull)
                       .filter(col("brand").isin("Эконика"))
                       .withColumn("rn", row_number().over(Window.partitionBy(col("merchant_nm"),
                         col("terminal_id"), col("latitude_mt"), col("longitude_mt"))
                         .orderBy(col("brand").desc_nulls_last, col("terminal_id").desc_nulls_last)))
                       .filter(col("rn") === 1)
                       .drop(col("rn"))
                       .withColumn("brand_count", sum(lit(1L)).over(Window.partitionBy(col("brand"))))
                       .withColumn("brand", when(col("brand") === "", "Остальное").otherwise(col("brand")))
                       .withColumn("rank", dense_rank().over(Window.orderBy(col("brand_count").desc, col("brand"))))
                       .withColumn("latitude_mt", col("latitude_mt").cast(DecimalType(18, 6)))
                       .withColumn("longitude_mt", col("longitude_mt").cast(DecimalType(18, 6)))
                       .cache)
    _          <- putStrLn(s"brandsMsc count: ${brandsMsc.count}")
    _          <- putStrLn(s"brandsMsc sample: ${brandsMsc.show(50, truncate = false)}")
    mapMoscow  <- Task(brandsMsc
                       .filter(col("brand_count") > 10)
                       .filter(col("latitude_mt") >= 55.55 && col("latitude_mt") <= 55.95)
                       .filter(col("longitude_mt") >= 37.35 && col("longitude_mt") <= 37.85)
                       .groupBy(col("latitude_mt"), col("longitude_mt"), col("brand"))
                       .agg(
                         max(col("merchant_nm")).as("merchant_nm"),
                         max(col("terminal_id")).as("terminal_id"),
                         max(col("brand_count")).as("brand_count"),
                         max(col("rank")).as("rank")
                       )
                       .select(
                         col("latitude_mt").as("Широта"),
                         col("longitude_mt").as("Долгота"),
                         concat_ws(" ", col("brand"), col("merchant_nm"),
                           col("terminal_id")).as("Описание"),
                         col("brand").as("Подпись"),
                         col("rank").as("Номер метки"),
                         col("brand_count"))
                       .orderBy(col("Номер метки").desc))
    _          <- putStrLn(s"mapMoscow count: ${mapMoscow.count}")
    _          <- putStrLn(s"mapMoscow sample: ${mapMoscow.show(50, truncate = false)}")
    _          <- SparkApp.writeData(conf.writeConf, mapMoscow
                  .select("Широта", "Долгота", "Описание", "Подпись", "Номер метки"), writePath).provide(spark)
    _          <- Task(spark.stop())
  } yield ()

  /*
   * Check Statistics on Result DataFrame cluster`s from Source BigData Census DataFrame
   */
  def checkRules(args: List[String]): ZIO[AppEnvironment, Throwable, Unit] = for {
    conf       <- config
    args       <- ArgumentsParser.parse(args)
    readPath    = args.flatMap(_.readPath)
    writePath   = args.flatMap(_.writePath)
    _          <- putStrLn(s"read path: $readPath")
    _          <- putStrLn(s"write path: $writePath")
    spark      <- createSparkSession("census-brands", 16)
    superCat    = "Дети и животные"
    text       <- SparkApp.readAsText(conf.readConf, readPath).provide(spark)
    _          <- putStrLn(s"count: ${text.count}")
    _          <- putStrLn(s"schema: ${text.printSchema}")
    _          <- putStrLn(s"sample: ${text.show(false)}")
    rdd        <- SparkApp.splitLineRDD(text.rdd, conf.readConf.fileConf.separator, CensusSchema.schema.length).provide(spark)
    data       <- SparkApp.createDataFrame(rdd, CensusSchema.schema).provide(spark)
    _          <- putStrLn(s"count: ${data.count}")
    _          <- putStrLn(s"schema: ${data.printSchema()}")
    _          <- putStrLn(s"partitions: ${data.rdd.getNumPartitions}")
    _          <- putStrLn(s"sample: ${data.show(false)}")
    rules      <- SparkApp.readExcel(conf.readConf, BrandsDictSchema.schema).provide(spark)
                  .map(_.filter(col("mcc_supercat") === superCat)
                        .cache)
    expr       <- SparkApp.createBrandExpression(rules, "brand", "reg_exp").provide(spark)
    _          <- putStrLn(s"expression:\n${SparkApp.BothSubstring(expr, 1000)}")
    udf        <- Task(udf((x: mutable.WrappedArray[String]) => x.length))
    brands     <- SparkApp.withBrandColumn(data, "brand", rules, "brand", "reg_exp").provide(spark)
                  .map(_.filter(col("mcc_supercat") === superCat)
                        .withColumn("area_mt", when(col("area_mt") === "\\N", null)
                          .otherwise(col("area_mt")))
                        .withColumn("locality_mt", when(col("locality_mt") === "\\N", null)
                          .otherwise(col("locality_mt")))
                        .withColumn("street_mt", when(col("street_mt") === "\\N", null)
                          .otherwise(col("street_mt")))
                        .withColumn("house_mt", when(col("house_mt") === "\\N", null)
                          .otherwise(col("house_mt")))
                        .withColumn("longitude_mt", when(col("longitude_mt") === "\\N", null)
                          .otherwise(col("longitude_mt")))
                        .withColumn("latitude_mt", when(col("latitude_mt") === "\\N", null)
                          .otherwise(col("latitude_mt")))
                        .withColumn("has_brand", col("brand").isNotNull)
                        .withColumn("has_address", col("area_mt").isNotNull || col("locality_mt").isNotNull ||
                          col("street_mt").isNotNull || col("house_mt").isNotNull)
                        .withColumn("has_crd", col("longitude_mt").isNotNull && col("latitude_mt").isNotNull)
                        .withColumn("mcc_clr", substring(SparkApp.clearColumn(col("mcc_name")), 0, 30))
                        .withColumn("merchant_clr", substring(SparkApp.clearColumn(col("merchant_nm")), 0, 30))
                        .withColumn("words", udf(split(col("merchant_clr"), " ")))
                        .withColumn("all_cnt", sum(lit(1L)).over(Window.partitionBy(lit(1L))))
                        .cache)
    _          <- putStrLn(s"count: ${brands.count}")
    _          <- putStrLn(s"schema: ${brands.printSchema}")
    _          <- putStrLn(s"partitions: ${brands.rdd.getNumPartitions}")
    _          <- putStrLn(s"sample: ${brands.show(100, truncate = false)}")
    brandsGrp  <- Task(brands
                       .groupBy(col("mcc_supercat"))
                       .agg(
                         max(col("all_cnt")).cast(DecimalType(24, 6)).as("all_cnt"),
                         sum(when(col("has_brand"), 1L).otherwise(0L)).cast(DecimalType(24, 6)).as("brand_cnt"),
                         sum(when(col("has_address"), 1L).otherwise(0L)).cast(DecimalType(24, 6)).as("address_cnt"),
                         sum(when(col("has_crd"), 1L).otherwise(0L)).cast(DecimalType(24, 6)).as("crd_cnt"))
                       .select(
                         col("mcc_supercat"),
                         col("all_cnt").cast(IntegerType).as("all_cnt"),
                         col("brand_cnt").cast(IntegerType).as("brand_cnt"),
                         col("address_cnt").cast(IntegerType).as("address_cnt"),
                         col("crd_cnt").cast(IntegerType).as("crd_cnt"),
                         ((col("brand_cnt") / col("all_cnt")) * 100.0).cast(DecimalType(24, 2)).as("brand_prc"),
                         ((col("address_cnt") / col("all_cnt")) * 100.0).cast(DecimalType(24, 2)).as("address_prc"),
                         ((col("crd_cnt") / col("all_cnt")) * 100.0).cast(DecimalType(24, 2)).as("crd_prc"))
                       .cache)
    _          <- putStrLn(s"stats count: ${brandsGrp.count}")
    _          <- putStrLn(s"stats sample: ${brandsGrp.show(10, truncate = false)}")
    _          <- Task(spark.stop())
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
    rdd        <- SparkApp.splitLineRDD(text2.rdd, conf.readConf.fileConf.separator, CensusBrandSchema.schema.length).provide(spark)
    data       <- SparkApp.createDataFrame(rdd, CensusBrandSchema.schema).provide(spark)
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
    _          <- Task(spark.stop())
  } yield ()
}
