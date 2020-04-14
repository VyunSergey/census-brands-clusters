package ru.sberbank.bigdata.mntz.census.brands

import zio._

package object conf {
  type Configuration = Has[Configuration.Service]

  val config: ZIO[Configuration, Throwable, Config] =
    ZIO.accessM(_.get.config)
}
