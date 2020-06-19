package com.vyunsergey.struct

import org.apache.spark.sql.types._

object OnlineMovementDictSchema {
  val schema: StructType = StructType(
    StructField("value", StringType, nullable = true) ::
    StructField("internet_flag", StringType, nullable = true) ::
    StructField("mcc_supercat", StringType, nullable = true) ::
    StructField("comment", StringType, nullable = true) :: Nil
  )
}
