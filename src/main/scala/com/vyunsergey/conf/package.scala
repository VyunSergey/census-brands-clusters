package com.vyunsergey

import zio._

package object conf {
  type Configuration = Has[Configuration.Service]

  val config: ZIO[Configuration, Throwable, Config] =
    ZIO.accessM(_.get.config)
}
