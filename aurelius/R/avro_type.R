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

# function to get PFA type names from R type names (FIXME: incomplete)

#' avro_type
#'
#' Inspects an R object and produces the corresponding Avro type name
#' @param obj object to inspect
#' @return a \code{list} of lists Avro schema
#' @examples
#' avro_type("hello")           # "string"
#' avro_type(factor("hello"))   # "string"
#' avro_type(3.14)              # "double"
#' avro_type(3)                 # "int"
#' @export
avro_type <- function(obj) {
    if (is.factor(obj))
        "string"
    else {
        t <- typeof(obj)
        if (t == "logical")
            "boolean"
        else if (t == "double")
            "double"
        else if (t == "integer")
            "int"
        else if (t == "character")
            "string"
        else if (t == "NULL")
            "null"
        else
            stop(paste("unsupported R -> PFA data type name conversion:", t))
    }
}