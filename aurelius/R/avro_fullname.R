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

# function to get fully qualified names and distinguish named types from unnamed types

#' avro_fullname
#'
#' Yields the full type name (with namespace) of an Avro \code{list} of lists
#' 
#' @param type Avro \code{list} of lists
#' @return string representing the full name
#' @examples
#' avro_fullname(avro_record(list(), "MyRecord"))                   # "MyRecord"
#' avro_fullname(avro_record(list(), "MyRecord", "com.wowzers"))    # "com.wowzers.MyRecord"
#' @export
avro_fullname <- function(type) {
  if (!is.atomic(type)  &&  !is.null(type$name)) {
    if (is.null(type$namespace)) {
        type$name
    } else {
        paste(type$namespace, type$name, sep=".")
    }
  } else {
    NULL
  }
}