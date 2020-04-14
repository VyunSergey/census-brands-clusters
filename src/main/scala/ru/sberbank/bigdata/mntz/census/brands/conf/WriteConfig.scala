package ru.sberbank.bigdata.mntz.census.brands.conf

final case class WriteConfig(
                              dataMap: Map[String, String],
                              fileConf: FileConfig
                            )
