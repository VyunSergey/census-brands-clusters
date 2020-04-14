package com.vyunsergey.conf

final case class FileConfig(
                             format: String,
                             mode: String,
                             encoding: String,
                             header: Boolean,
                             inferSchema: Boolean,
                             separator: String
                           )
