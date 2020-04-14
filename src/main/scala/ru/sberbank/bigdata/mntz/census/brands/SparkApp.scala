package ru.sberbank.bigdata.mntz.census.brands

import com.crealytics.spark.excel._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import ru.sberbank.bigdata.mntz.census.brands.conf._
import zio._

object SparkApp {
  def readData(config: ReadConfig,
               schema: StructType,
               path: Option[String] = None): ZIO[SparkSession, Throwable, DataFrame] = for {
    spark <- ZIO.environment[SparkSession]
    data <- Task(
      spark.read
        .format(config.fileConf.format)
        .schema(schema)
        .options(Map(
          "mode" -> config.fileConf.mode,
          "encoding" -> config.fileConf.encoding,
          "header" -> config.fileConf.header.toString,
          "sep" -> config.fileConf.separator,
          "inferSchema" -> config.fileConf.inferSchema.toString
        ))
        .load(path.getOrElse(config.dataMap("result")))
    )
  } yield data

  def readAsText(config: ReadConfig,
                 path: Option[String] = None): ZIO[SparkSession, Throwable, DataFrame] = for {
    spark <- ZIO.environment[SparkSession]
    data <- Task(
      spark.read
        .options(Map(
          "encoding" -> config.fileConf.encoding,
          "header" -> config.fileConf.header.toString,
          "sep" -> "$",
          "inferSchema" -> "false"
        ))
        .csv(path.getOrElse(config.dataMap("source")))
    )
  } yield data

  def readExcel(config: ReadConfig,
                schema: StructType,
                path: Option[String] = None): ZIO[SparkSession, Throwable, DataFrame] = for {
    spark <- ZIO.environment[SparkSession]
    data <- Task(
      spark.read
        .excel(
          header = config.excelConf.header,
          treatEmptyValuesAsNulls = config.excelConf.treatEmptyValuesAsNulls,
          inferSchema = config.excelConf.inferSchema,
          addColorColumns = config.excelConf.addColorColumns,
          dataAddress = config.excelConf.dataAddress,
          timestampFormat = config.excelConf.timestampFormat,
          maxRowsInMemory = config.excelConf.maxRowsInMemory,
          excerptSize = config.excelConf.excerptSize
        )
        .schema(schema)
        .load(path.getOrElse(config.dataMap("dict")))
    )
  } yield data

  def writeData(config: WriteConfig,
                data: DataFrame,
                path: Option[String],
                colNames: List[String] = Nil): ZIO[SparkSession, Throwable, Unit] = {
    Task(
      colNames.length match {
        case l if l > 0 =>
          data.repartition(colNames.map(col): _*)
            .write
            .format(config.fileConf.format)
            .mode(config.fileConf.mode)
            .partitionBy(colNames: _*)
            .options(Map(
              "encoding" -> config.fileConf.encoding,
              "header" -> config.fileConf.header.toString,
              "sep" -> config.fileConf.separator
            ))
            .save(path.getOrElse(config.dataMap("temp")))
        case _ =>
          data.repartition(1)
            .write
            .format(config.fileConf.format)
            .mode(config.fileConf.mode)
            .options(Map(
              "encoding" -> config.fileConf.encoding,
              "header" -> config.fileConf.header.toString,
              "sep" -> config.fileConf.separator
            ))
            .save(path.getOrElse(config.dataMap("temp")))
      }
    )
  }

  def countGroupBy(data: DataFrame,
                   colNames: String*): ZIO[SparkSession, Throwable, DataFrame] = {
    Task(
      data.groupBy(colNames.map(col): _*)
        .count()
        .select(colNames.map(col) :+ col("count"): _*)
        .orderBy(col("count").desc)
    )
  }

  def countStatistics(data: DataFrame,
                      colNames: String*): ZIO[SparkSession, Throwable, DataFrame] = {
    Task(
      data.groupBy(colNames.map(col): _*)
        .count()
        .select(
          colNames.map(col) ++
          colNames.map({nm =>
            sum(col("count")).over(Window.partitionBy(col(nm))).alias(s"${nm}_count")
          }): _*)
        .orderBy(colNames.map(nm => col(s"${nm}_count").desc): _*)
    )
  }

  def splitLineRDD(rdd: RDD[Row],
                   separator: String): ZIO[SparkSession, Throwable, RDD[Row]] = {
    Task(
      rdd.map({row =>
        val line: List[String] = row.getString(0).split(separator).toList
        if (line.length == 17) {
          Row(line.head, line(1), line(2), line(3), line(4), line(5), line(6), line(7), line(8),
            line(9), line(10), line(11), line(12), line(13), line(14), line(15), line(16))
        } else {
          Row("NULL")
        }
      }).filter(row => row != Row("NULL"))
    )
  }

  def createDataFrame(rdd: RDD[Row],
                      schema: StructType): ZIO[SparkSession, Throwable, DataFrame] = for {
    spark <- ZIO.environment[SparkSession]
    result <- Task(spark.createDataFrame(rdd, schema))
  } yield result

  def createDataFrame(data: DataFrame,
                      schema: StructType): ZIO[SparkSession, Throwable, DataFrame] =
    createDataFrame(data.rdd, schema)

  def createPart(name: String,
                 num: Int): Column = {
    concat_ws("_", lit(name), lit(num).cast(StringType))
  }

  def filterRow(data: DataFrame,
                row: Row): ZIO[SparkSession, Throwable, DataFrame] = {
    Task(data.filter(r => r != row))
  }

  def filterData(data: DataFrame,
                 colName: String,
                 filter: Option[String]): ZIO[SparkSession, Throwable, DataFrame] = {
    Task(
      filter match {
        case Some(f) => data.filter(col(colName).eqNullSafe(f))
        case None    => data.filter(col(colName).isNull)
      }
    )
  }

  def filterRlikeData(data: DataFrame,
                      colName: String,
                      filter: Option[String]): ZIO[SparkSession, Throwable, DataFrame] = {
    Task(
      filter match {
        case Some(f) => data.filter(col(colName).rlike(f))
        case None    => data.filter(col(colName).isNull)
      }
    )
  }

  def filterRlikeNull(data: DataFrame,
                      colName: String): ZIO[SparkSession, Throwable, DataFrame] = {
    Task(
      data
        .filter(!col(colName).rlike("Поездки, доставка, хранение"))
        .filter(!col(colName).rlike("Супермаркеты и продуктовые магазины"))
        .filter(!col(colName).rlike("Телефония и интернет"))
        .filter(!col(colName).rlike("Всё остальное"))
        .filter(!col(colName).rlike("Интернет-магазины"))
        .filter(!col(colName).rlike("Всё для дома"))
        .filter(!col(colName).rlike("Общепит"))
        .filter(!col(colName).rlike("Одежда, обувь и аксессуары"))
        .filter(!col(colName).rlike("Медицина и косметика"))
        .filter(!col(colName).rlike("Личный транспорт"))
        .filter(!col(colName).rlike("Оплата счетов"))
        .filter(!col(colName).rlike("Дети и животные"))
    )
  }

  def LeftSubstring(str: String, len: Int): String = {
    val ind =
      if(0 <= len && len <= str.length) len
      else if(len > str.length) str.length
      else 0
    str.substring(0, ind)
  }

  def RightSubstring(str: String, len: Int): String = {
    val ind =
      if(0 <= len && len <= str.length) len
      else if(len > str.length) str.length
      else 0
    str.substring(str.length - ind, str.length)
  }

  def BothSubstring(str: String, len: Int): String = {
    if (0 <= len && len <= str.length) LeftSubstring(str, len / 2) + "\n...\n" + RightSubstring(str, len / 2)
    else str
  }

  def getFirst(data: DataFrame): ZIO[SparkSession, Throwable, Row] = {
    Task(data.first())
  }

  def withExprColumn(data: DataFrame,
                     colName: String,
                     expression: String): ZIO[SparkSession, Throwable, DataFrame] = {
    Task(data.withColumn(colName, expr(expression)))
  }

  def createBrandExpression(data: DataFrame,
                            brandColName: String,
                            exprColName: String): ZIO[SparkSession, Throwable, String] = for {
    spark <- ZIO.environment[SparkSession]
    expr <- Task({
      import spark.implicits._

      data.select(
        col(exprColName).as[String],
        col(brandColName).as[String]
      ).collect().toList
        .map({case (exp, brand) =>
          s"when $exp then '${brand.replaceAll("'", "`")}'"
        })
        .mkString("\n")
    })
  } yield {
    if (expr.nonEmpty) s"case $expr end"
    else "cast(null as string)"
  }

  def withBrandColumn(data: DataFrame,
                      colName: String,
                      dictData: DataFrame,
                      brandColName: String,
                      exprColName: String): ZIO[SparkSession, Throwable, DataFrame] = for {
    expr <- createBrandExpression(dictData, brandColName, exprColName)
    data <- withExprColumn(data, colName, expr)
  } yield data

  def transformData(data: DataFrame): ZIO[SparkSession, Throwable, DataFrame] = {
    val travelFilter         = col("mcc_supercat").eqNullSafe("Поездки, доставка, хранение")
    val superMarketFilter    = col("mcc_supercat").eqNullSafe("Супермаркеты и продуктовые магазины")
    val phoneInternetFilter  = col("mcc_supercat").eqNullSafe("Телефония и интернет")
    val otherFilter          = col("mcc_supercat").eqNullSafe("Всё остальное")
    val internetMarketFilter = col("mcc_supercat").eqNullSafe("Интернет-магазины")
    val homeFilter           = col("mcc_supercat").eqNullSafe("Всё для дома")
    val burgerFilter         = col("mcc_supercat").eqNullSafe("Общепит")
    val clothesFilter        = col("mcc_supercat").eqNullSafe("Одежда, обувь и аксессуары")
    val medicineFilter       = col("mcc_supercat").eqNullSafe("Медицина и косметика")
    val carFilter            = col("mcc_supercat").eqNullSafe("Личный транспорт")
    val billFilter           = col("mcc_supercat").eqNullSafe("Оплата счетов")
    val childrenAnimalFilter = col("mcc_supercat").eqNullSafe("Дети и животные")
    val nullFilter           = col("mcc_supercat").isNull

    val travelAircraftPart = "Авиакомпании" :: Nil
    val travelAircraftNoNamePart = "Авиакомпании – не имеющие своих кодов" :: Nil
    val travelTransitPart = "Перевозки, доставка, хранение" :: "Автобусные перевозки" :: Nil
    val travelAircraftParts =
      when(col("mcc_name").isin(travelAircraftNoNamePart: _*), createPart("travel", 1))
        .otherwise(createPart("travel", 2))
    val travelParts =
      when(col("mcc_cat").isin(travelAircraftPart: _*), travelAircraftParts)
        .when(col("mcc_cat").isin(travelTransitPart: _*), createPart("travel", 3))
        .otherwise(createPart("travel", 4))

    val superMarketProductPart = "Супермаркеты и продукты" :: Nil
    val superMarketStorePart = "Разные продуктовые магазины" :: Nil
    val superMarketAlcoholPart = "Магазины алкоголя" :: Nil
    val superMarketParts =
      when(col("mcc_cat").isin(superMarketProductPart: _*), createPart("super_market", 1))
        .when(col("mcc_cat").isin(superMarketStorePart: _*), createPart("super_market", 2))
        .when(col("mcc_cat").isin(superMarketAlcoholPart: _*), createPart("super_market", 3))
        .otherwise(createPart("super_market", 4))

    val phoneInternetPaymentPart = "Связь - оплата мобильных телефонов" :: Nil
    val phoneInternetSalePart = "Связь - продажа телефонов и оборудования" :: "Связь - доступ в интернет" :: Nil
    val phoneInternetPaymentParts =
      when(col("merchant_nm").rlike("MEGAFON"), createPart("phone_internet", 1))
        .when(col("merchant_nm").rlike("MTS"), createPart("phone_internet", 1))
        .when(col("merchant_nm").rlike("BEELINE"), createPart("phone_internet", 1))
        .when(col("merchant_nm").rlike("TELE2"), createPart("phone_internet", 1))
        .otherwise(createPart("phone_internet", 2))
    val phoneInternetParts =
      when(col("mcc_cat").isin(phoneInternetPaymentPart: _*), phoneInternetPaymentParts)
        .when(col("mcc_cat").isin(phoneInternetSalePart: _*), createPart("phone_internet", 3))
        .otherwise(createPart("phone_internet", 4))

    val otherServicePart = "Разные услуги" :: Nil
    val otherProductPart = "Книги, канцтовары, фото" ::
      "Часы, мех, ювелирные изделия, искусство" ::
      "Магазины цветов" :: Nil
    val otherBeautySportPart = "Парикмахерские и салоны красоты" ::
      "Всё для спорта" ::
      "Разные развлечения" ::
      "Образовательные услуги" ::
      "Ломбарды и подержанные товары" :: Nil
    val otherParts =
      when(col("mcc_cat").isin(otherServicePart: _*), createPart("other", 1))
        .when(col("mcc_cat").isin(otherProductPart: _*), createPart("other", 2))
        .when(col("mcc_cat").isin(otherBeautySportPart: _*), createPart("other", 3))
        .otherwise(createPart("other", 4))

    val internetMarketProductPart = "Интернет-покупки разных товаров" :: Nil
    val internetMarketParts =
      when(col("mcc_cat").isin(internetMarketProductPart: _*), createPart("internet_market", 1))
        .otherwise(createPart("internet_market", 2))

    val homeAllPart = "Всё для дома" :: Nil
    val homeBuildPart = "Строительство и ремонт" :: Nil
    val homeParts =
      when(col("mcc_cat").isin(homeAllPart: _*), createPart("home", 1))
        .when(col("mcc_cat").isin(homeBuildPart: _*), createPart("home", 2))
        .otherwise(createPart("home", 3))

    val burgerCafePart = "Общепит" :: Nil
    val burgerCafeFastFoodPart = "Фастфуд" :: Nil
    val burgerCafeParts =
      when(col("mcc_name").isin(burgerCafeFastFoodPart: _*), createPart("burger", 1))
        .otherwise(createPart("burger", 2))
    val burgerParts =
      when(col("mcc_cat").isin(burgerCafePart: _*), burgerCafeParts)
        .otherwise(createPart("burger", 3))

    val clothesStuffPart = "Одежда и аксессуары" :: "Обувь" :: Nil
    val clothesStuffStorePart = "Магазины мужской и женской одежды" ::
      "Магазины женской одежды" :: "Магазины одежды для всей семьи" :: Nil
    val clothesStuffParts =
      when(col("mcc_name").isin(clothesStuffStorePart: _*), createPart("clothes", 1))
        .otherwise(createPart("clothes", 2))
    val clothesParts =
      when(col("mcc_cat").isin(clothesStuffPart: _*), clothesStuffParts)
        .otherwise(createPart("clothes", 3))

    val medicineStorePart = "Аптеки" :: Nil
    val medicineParts =
      when(col("mcc_cat").isin(medicineStorePart: _*), createPart("medicine", 1))
        .otherwise(createPart("medicine", 2))

    val carSalePart = "Продажа и обслуживание личного транспорта" :: Nil
    val carGasStationPart = "Автозаправки" :: Nil
    val carParts =
      when(col("mcc_cat").isin(carSalePart: _*), createPart("car", 1))
        .when(col("mcc_cat").isin(carGasStationPart: _*), createPart("car", 2))
        .otherwise(createPart("car", 3))

    val billCommunalPart = "Оплата счетов" :: "Коммунальные услуги" :: Nil
    val billParts =
      when(col("mcc_cat").isin(billCommunalPart: _*), createPart("bill", 1))
        .otherwise(createPart("bill", 2))

    val childrenAnimalAllPart = "Всё для детей" :: "Всё для домашних животных" :: Nil
    val childrenAnimalParts =
      when(col("mcc_cat").isin(childrenAnimalAllPart: _*), createPart("children_animal", 1))
        .otherwise(createPart("children_animal", 2))

    val nullParts =
      when(col("mcc_cat").isNull, createPart("none", 1))
        .otherwise(createPart("none", 2))

    val brandParts = when(travelFilter, travelParts)
      .when(superMarketFilter,    superMarketParts)
      .when(phoneInternetFilter,  phoneInternetParts)
      .when(otherFilter,          otherParts)
      .when(internetMarketFilter, internetMarketParts)
      .when(homeFilter,           homeParts)
      .when(burgerFilter,         burgerParts)
      .when(clothesFilter,        clothesParts)
      .when(medicineFilter,       medicineParts)
      .when(carFilter,            carParts)
      .when(billFilter,           billParts)
      .when(childrenAnimalFilter, childrenAnimalParts)
      .when(nullFilter,           nullParts)
      .otherwise(lit(0))
      .alias("brands_parts")

    Task(
      data.withColumn("brands_part", brandParts)
        .repartition(col("brands_part"))
        .select(
          ("brands_part" +: data.columns).map(col): _*
        )
    )
  }
}
