# Copyright (C) 2014  Open Data ("Open Data" refers to
# one or more of the following companies: Open Data Partners LLC,
# Open Data Research LLC, or Open Data Capital LLC.)
# 
# This file is part of Hadrian.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' avro_null
#'
#' Constructs a \code{list} of lists Avro schema for the null type (type with only one value).
#'
#' @name avro_null
#' @export
avro_null <- "null"

#' avro_boolean
#'
#' Constructs a \code{list} of lists Avro schema for the boolean (logical) type.
#'
#' @name avro_boolean
#' @export
avro_boolean <- "boolean"

#' avro_int
#'
#' Constructs a \code{list} of lists Avro schema for the int (integer numeric with 32-bit precision) type.
#'
#' @name avro_int
#' @export
avro_int <- "int"

#' avro_long
#'
#' Constructs a \code{list} of lists Avro schema for the long (integer numeric with 64-bit precision) type.
#'
#' @name avro_long
#' @export
avro_long <- "long"

#' avro_float
#'
#' Constructs a \code{list} of lists Avro schema for the float (floating-point numeric with 32-bit precision) type.
#'
#' @name avro_float
#' @export
avro_float <- "float"

#' avro_double
#'
#' Constructs a \code{list} of lists Avro schema for the double (floating-point numeric with 64-bit precision) type.
#'
#' @name avro_double
#' @export
avro_double <- "double"

#' avro_bytes
#'
#' Constructs a \code{list} of lists Avro schema for the bytes (unstructured byte array) type.
#'
#' @name avro_bytes
#' @export
avro_bytes <- "bytes"

#' avro_fixed
#'
#' Constructs a \code{list} of lists Avro schema for the fixed (byte array with fixed size) type.
#' @source gen_unique_name.R
#' @param size size of the byte array
#' @param name required name (if missing, \code{\link{gen_unique_fixed_name}} is invoked)
#' @param namespace optional namespace
#' @examples
#' avro_fixed(6, "MACAddress")
#' @export
avro_fixed <- function(size, name = NULL, namespace = NULL) {
    
  if (is.null(name)){
    name <- gen_unique_fixed_name()
  }

  if (is.null(namespace)){
    list(type = "fixed", size = size, name = name)
  } else {
    list(type = "fixed", size = size, name = name, namespace = namespace)
  }
}

#' avro_string
#'
#' Constructs a \code{list} of lists Avro schema for the string (UTF-8) type.
#'
#' @name avro_string
#' @export
avro_string <- "string"

#' avro_enum
#'
#' Constructs a \code{list} of lists Avro schema for the enum (set of symbols) type.
#' @source gen_unique_name.R
#' @param symbols list of string-valued symbol names
#' @param name required name (if missing, \code{\link{gen_unique_enum_name}} is invoked)
#' @param namespace optional namespace
#' @examples
#' avro_enum(list("one", "two", "three"))
#' @export
avro_enum <- function(symbols, name = NULL, namespace = NULL){
  
  if (is.null(name)){
    name <- gen_unique_enum_name()
  }
  
  if (is.null(namespace)){
    list(type = "enum", symbols = symbols, name = name)
  } else {
    list(type = "enum", symbols = symbols, name = name, namespace = namespace)
  }
}

#' avro_array
#'
#' Constructs a \code{list} of lists Avro schema for the array type.
#' @param items schema for the homogeneous array
#' @examples
#' avro_array(avro_int)
#' avro_array(avro_string)
#' @export
avro_array <- function(items) {
  list(type = "array", items = items)
}

#' avro_map
#'
#' Constructs a \code{list} of lists Avro schema for the map type.
#' @param values schema for the homogeneous map
#' @examples
#' avro_map(avro_int)
#' avro_map(avro_string)
#' @export
avro_map <- function(values) {
  list(type = "map", values = values)
}

#' avro_record
#'
#' Constructs a \code{list} of lists Avro schema for the record type.
#' @param fields named list of field names and schemas
#' @param name required name (if missing, gen_unique_rec_name is invoked)
#' @param namespace optional namespace
#' @examples
#' avro_record(list(one = avro_int, two = avro_double, three = avro_string))
#' @export
avro_record <- function(fields, name = NULL, namespace = NULL) {
    
  outputFields <- list()
    
  for (x in names(fields)){
    outputFields[[length(outputFields) + 1]] = list(name = x, type = fields[[x]])
  }
  
  if (is.null(name)){
    name <- gen_unique_rec_name()
  }
  
  if (is.null(namespace)){
    list(type = "record", fields = outputFields, name = name)
  } else {
    list(type = "record", fields = outputFields, name = name, namespace = namespace)
  }
}

#' avro_union
#'
#' Constructs a \code{list} of lists Avro schema for the tagged union type.
#' @param ... schemas for each of the possible sub-types
#' @examples
#' avro_union(avro_null, avro_int)         # a way to make a nullable int
#' avro_union(avro_double, avro_string)    # any set of types can be unioned
#' @export
avro_union <- function(...) list(...)