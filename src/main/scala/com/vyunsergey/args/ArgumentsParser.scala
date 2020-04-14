package com.vyunsergey.args

import zio._

object ArgumentsParser {
  def usage(): Unit = {
    println(
      """Usage:
        |  main --read-path <path-to-read-data> --write-path <path-to-write-data>
        |""".stripMargin)
  }

  def parse(args: List[String]): ZIO[ZEnv, Nothing, Option[Arguments]] = {
    Task(
      args match {
        case "--read-path" :: readPath ::
             "--write-path" :: writePath :: Nil => Some(Arguments(Some(readPath), Some(writePath)))
        case "--write-path" :: writePath ::
          "--read-path" :: readPath :: Nil => Some(Arguments(Some(readPath), Some(writePath)))
        case "--read-path" :: readPath :: Nil => Some(Arguments(Some(readPath), None))
        case "--write-path" :: writePath :: Nil => Some(Arguments(None, Some(writePath)))
        case _ =>
          usage()
          None
      }
    ).orElseSucceed({
        usage()
        None
    })
  }
}
