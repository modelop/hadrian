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

# function to handle the define-type-once rule

#' avro_typemap
#'
#' Convenience function for ensuring that Avro type schemas are declared exactly 
#' once. It returns a function that yields a full type declaration the first 
#' time it is invoked and just a name on subsequent times.
#' @param ... key-value pairs of Avro type schemas
#' @return a function that yields Avro type schemas or just their names
#' @examples
#' tm <- avro_typemap(
#'     MyType1 = avro_record(list(one = avro_int, two = avro_double, three = avro_string)),
#'     MyType2 = avro_array(avro_double)
#' )
#' tm("MyType1")           # produces the whole declaration
#' tm("MyType1")           # produces just "MyType1"
#' tm("MyType2")           # produces the whole declaration
#' tm("MyType2")           # produces the declaration again because this is not a named type
#' @export
avro_typemap <- function(...) {
    types <- list(...)

    # closure over this counter keeps track of how many times we've seen a name
    counter <- new.env()
    for (name in names(types))
        if (!is.null(avro_fullname(types[[name]])))
            counter[[name]] = 0

    function(name) {
        if (name %in% ls(counter)) {
            out <- if (counter[[name]] == 0) types[[name]] else avro_fullname(types[[name]])
            counter[[name]] = counter[[name]] + 1
            out
        }
        else
            types[[name]]
    }
}