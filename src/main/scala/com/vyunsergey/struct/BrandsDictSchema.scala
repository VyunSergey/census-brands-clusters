package com.vyunsergey.struct

import org.apache.spark.sql.types._

object BrandsDictSchema {
  val schema: StructType = StructType(
    StructField("num", IntegerType, nullable = true) ::
      StructField("reg_exp", StringType, nullable = true) ::
      StructField("brand", StringType, nullable = true) ::
      StructField("mcc_supercat", StringType, nullable = true) ::
      StructField("comment", StringType, nullable = true) :: Nil
  )
}
