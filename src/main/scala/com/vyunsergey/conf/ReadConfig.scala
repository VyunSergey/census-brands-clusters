package com.vyunsergey.conf

final case class ReadConfig(
                             dataMap: Map[String, String],
                             fileConf: FileConfig,
                             excelConf: ExcelConfig
                           )
