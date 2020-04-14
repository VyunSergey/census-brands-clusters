package com.vyunsergey.conf

final case class WriteConfig(
                              dataMap: Map[String, String],
                              fileConf: FileConfig
                            )
