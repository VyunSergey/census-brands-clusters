package ru.sberbank.bigdata.mntz.census.brands.conf

final case class ReadConfig(
                             dataMap: Map[String, String],
                             fileConf: FileConfig,
                             excelConf: ExcelConfig
                           )
