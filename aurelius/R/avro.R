# Copyright (C) 2014  Open Data ("Open Data" refers to
# one or more of the following companies: Open Data Partners LLC,
# Open Data Research LLC, or Open Data Capital LLC.)
# 
# This file is part of Hadrian.
# 
# Licensed under the Hadrian Personal Use and Evaluation License (PUEL);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://raw.githubusercontent.com/opendatagroup/hadrian/master/LICENSE
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' avro.null
#'
#' Constructs a list-of-lists Avro schema for the null type (type with only one value).
#'
#' @export avro.null
#' @name avro.null

avro.null <- "null"

#' avro.boolean
#'
#' Constructs a list-of-lists Avro schema for the boolean (logical) type.
#'
#' @export avro.boolean
#' @name avro.boolean

avro.boolean <- "boolean"

#' avro.int
#'
#' Constructs a list-of-lists Avro schema for the int (integer numeric with 32-bit precision) type.
#'
#' @export avro.int
#' @name avro.int

avro.int <- "int"

#' avro.long
#'
#' Constructs a list-of-lists Avro schema for the long (integer numeric with 64-bit precision) type.
#'
#' @export avro.long
#' @name avro.long

avro.long <- "long"

#' avro.float
#'
#' Constructs a list-of-lists Avro schema for the float (floating-point numeric with 32-bit precision) type.
#'
#' @export avro.float
#' @name avro.float

avro.float <- "float"

#' avro.double
#'
#' Constructs a list-of-lists Avro schema for the double (floating-point numeric with 64-bit precision) type.
#'
#' @export avro.double
#' @name avro.double

avro.double <- "double"

#' avro.bytes
#'
#' Constructs a list-of-lists Avro schema for the bytes (unstructured byte array) type.
#'
#' @export avro.bytes
#' @name avro.bytes

avro.bytes <- "bytes"

#' avro.fixed
#'
#' Constructs a list-of-lists Avro schema for the fixed (byte array with fixed size) type.
#' @param size size of the byte array
#' @param name required name (if missing, uniqueFixedName is invoked)
#' @param namespace optional namespace
#' @export avro.fixed
#' @examples
#' avro.fixed(6, "MACAddress")

avro.fixed <- function(size, name = NULL, namespace = NULL) {
    if (is.null(name))
        name <- uniqueFixedName()
    if (is.null(namespace))
        list(type = "fixed", size = size, name = name)
    else
        list(type = "fixed", size = size, name = name, namespace = namespace)
}

#' avro.string
#'
#' Constructs a list-of-lists Avro schema for the string (UTF-8) type.
#'
#' @export avro.string
#' @name avro.string

avro.string <- "string"

#' avro.enum
#'
#' Constructs a list-of-lists Avro schema for the enum (set of symbols) type.
#' @param symbols list of string-valued symbol names
#' @param name required name (if missing, uniqueEnumName is invoked)
#' @param namespace optional namespace
#' @export avro.enum
#' @examples
#' avro.enum(list("one", "two", "three"))

avro.enum <- function(symbols, name = NULL, namespace = NULL) {
    if (is.null(name))
        name <- uniqueEnumName()
    if (is.null(namespace))
        list(type = "enum", symbols = symbols, name = name)
    else
        list(type = "enum", symbols = symbols, name = name, namespace = namespace)
}

#' avro.array
#'
#' Constructs a list-of-lists Avro schema for the array type.
#' @param items schema for the homogeneous array
#' @export avro.array
#' @examples
#' avro.array(avro.int)
#' avro.array(avro.string)

avro.array <- function(items) {
    list(type = "array", items = items)
}

#' avro.map
#'
#' Constructs a list-of-lists Avro schema for the map type.
#' @param values schema for the homogeneous map
#' @export avro.map
#' @examples
#' avro.map(avro.int)
#' avro.map(avro.string)

avro.map <- function(values) {
    list(type = "map", values = values)
}

#' avro.record
#'
#' Constructs a list-of-lists Avro schema for the record type.
#' @param fields named list of field names and schemas
#' @param name required name (if missing, uniqueRecordName is invoked)
#' @param namespace optional namespace
#' @export avro.record
#' @examples
#' avro.record(list(one = avro.int, two = avro.double, three = avro.string))

avro.record <- function(fields, name = NULL, namespace = NULL) {
    outputFields <- list()
    for (x in names(fields))
        outputFields[[length(outputFields) + 1]] = list(name = x, type = fields[[x]])
    if (is.null(name))
        name <- uniqueRecordName()
    if (is.null(namespace))
        list(type = "record", fields = outputFields, name = name)
    else
        list(type = "record", fields = outputFields, name = name, namespace = namespace)
}

#' avro.union
#'
#' Constructs a list-of-lists Avro schema for the tagged union type.
#' @param args schemas for each of the possible sub-types
#' @export avro.union
#' @examples
#' avro.union(avro.null, avro.int)         # a way to make a nullable int
#' avro.union(avro.double, avro.string)    # any set of types can be unioned

avro.union <- function(...) list(...)
