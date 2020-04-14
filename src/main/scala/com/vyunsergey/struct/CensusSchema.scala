package com.vyunsergey.struct

import org.apache.spark.sql.types._

object CensusSchema {
  val schema: StructType = StructType(
    StructField("merchant_nm", StringType, nullable = true) ::
    StructField("merchant_id", StringType, nullable = true) ::
    StructField("inn_mt", StringType, nullable = true) ::
    StructField("terminal_id", StringType, nullable = true) ::
    StructField("mcc_supercat", StringType, nullable = true) ::
    StructField("mcc_cat", StringType, nullable = true) ::
    StructField("mcc_code", StringType, nullable = true) ::
    StructField("mcc_name", StringType, nullable = true) ::
    StructField("merchant_city_nm", StringType, nullable = true) ::
    StructField("province_stl", StringType, nullable = true) ::
    StructField("province_mt", StringType, nullable = true) ::
    StructField("area_mt", StringType, nullable = true) ::
    StructField("locality_mt", StringType, nullable = true) ::
    StructField("street_mt", StringType, nullable = true) ::
    StructField("house_mt", StringType, nullable = true) ::
    StructField("longitude_mt", StringType, nullable = true) ::
    StructField("latitude_mt", StringType, nullable = true) :: Nil
  )
}
