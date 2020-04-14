package ru.sberbank.bigdata.mntz.census.brands.args

final case class Arguments(
                            readPath: Option[String],
                            writePath: Option[String]
                          )
