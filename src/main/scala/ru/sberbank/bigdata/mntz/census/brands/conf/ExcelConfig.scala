package ru.sberbank.bigdata.mntz.census.brands.conf

final case class ExcelConfig(
                              header: Boolean,
                              treatEmptyValuesAsNulls: Boolean,
                              inferSchema: Boolean,
                              addColorColumns: Boolean,
                              dataAddress: String,
                              timestampFormat: String,
                              maxRowsInMemory: Int,
                              excerptSize: Int
                            )
