package ru.sberbank.bigdata.mntz.census.brands.conf

import pureconfig._
import pureconfig.generic.ProductHint
import pureconfig.generic.auto._
import zio._

object Configuration {

  trait Service {
    val config: Task[Config]
  }

  trait Live extends Configuration.Service {
    implicit def hint[T]: ProductHint[T] = ProductHint[T](ConfigFieldMapping(CamelCase, KebabCase))

    val config: Task[Config] = Task(ConfigSource.default.loadOrThrow[Config])
  }

  val live: ZLayer[Any, Nothing, Configuration] = ZLayer.succeed(new Live {})
}
